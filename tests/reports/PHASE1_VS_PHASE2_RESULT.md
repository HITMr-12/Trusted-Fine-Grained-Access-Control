# Phase 1 vs Phase 2 对比测试报告（初版）

日期：2026-09-11。两节点拓扑：E（引擎节点，ARM64，Spark 3.5.8 + Iceberg 1.7.2）/
R（172.168.22.25，MinIO 9100 + Polaris minimal 8181 + Remote 8002，ARM64）。

## 环境

| 项 | 值 |
|---|---|
| 数据 | NYC TLC yellow_tripdata_2024-01，296.4 万行，Iceberg 表 `fgac.nyc.taxi_trips`（snapshot 6896028106384817739，3 数据文件） |
| Phase 1 | Spark 经 s3a 跨机 Iceberg scan，SQL 内显式 `WHERE VendorID = N` |
| Phase 2 | Spark 插件协议 `/v2/subplans` → Remote(R) → Polaris 授权（alice→VendorID=1，bob→2）→ Remote 本地 Iceberg scan → 内联 JSON 回传 |
| 负载 | 5 SQL × 2 主体，预热 2 次、重复 5 次取 p50，Phase 2 并发固定 1 |

## 结果（p50 延迟）

| query | principal | rows | P1 | P2 | 减速 |
|---|---|---:|---:|---:|---:|
| Q1_scan_all | alice | 729,732 | 335ms | 15,428ms | 46.0x |
| Q2_high_sel | alice | 36,149 | 380ms | 988ms | 2.6x |
| Q3_low_sel | alice | 713,392 | 341ms | 15,024ms | 44.1x |
| Q4_projection | alice | 729,732 | 207ms | 10,777ms | 52.0x |
| Q5_compound | alice | 138,064 | 359ms | 3,035ms | 8.5x |
| Q1_scan_all | bob | 2,234,632 | 238ms | 46,948ms | 197.4x |
| Q2_high_sel | bob | 151,891 | 287ms | 3,297ms | 11.5x |
| Q3_low_sel | bob | 2,157,883 | 276ms | 44,569ms | 161.2x |
| Q4_projection | bob | 2,234,632 | 195ms | 32,786ms | 168.1x |
| Q5_compound | bob | 436,972 | 331ms | 9,161ms | 27.6x |

两阶段行数完全一致（含 alice Q2=36,149、bob Q1=2,234,632 等），语义对齐验证通过。

## 结论

1. **FGAC 开销与返回行数强相关**：减速比从 2.6x（3.6 万行）到 197x（223 万行）。
   瓶颈是 Remote 的内联 JSON 编码回传（`remote/app.py` 的 `inline_rows`），
   与文档 `perf-test-plan.md` 第 9 节预判一致。
2. **低选择率（大量返回行）是架构性成本**：223 万行 → JSON 序列化 + HTTP 明文
   传输 + E 侧解析占绝对主导；高选择率（3.6 万行）时链路开销可见且可接受
   （约 1 秒）。
3. 该结果是**当前实现**的FGAC 成本上界：换成 Arrow Flight / 结果 Parquet +
   TTL 等流式回传后，大量行场景的倍数预期显著下降（对应文档演进路线）。

## 后续

- 双机 CPU 采样已留存（/tmp/fgac-cpu-p1*.log、/tmp/fgac-cpu-p2*.log），可回溯
  分析 E/R 分担比例。
- 并发 4 的 Phase 2（远程并发执行）未纳入本报告，作为下一轮工作。
- 原始数据：/tmp/fgac-bench-phase1/phase1_results.json、
  /tmp/fgac-bench-phase2/phase2_results.json。

## 追加：Parquet 结果物化复测（2026-09-12）

将 Remote 回传从内联 JSON 改为查询级 Parquet 结果对象（`remote/app.py`
`materialize_result`：结果写 `s3a://fgac/results/<uuid>`，返回带 TTL 的
查询级凭据信封；E 侧 `tests/phase2p_fgac_benchmark.py` 凭信封经 s3a 读取），
同一负载复测（p50）：

| query | principal | rows | P1 | P2(JSON) | P2P(Parquet) | JSON减速 | Parquet减速 |
|---|---|---:|---:|---:|---:|---:|---:|
| Q1_scan_all | alice | 729,732 | 335ms | 15,428ms | 1,228ms | 46.0x | **3.7x** |
| Q2_high_sel | alice | 36,149 | 380ms | 988ms | 921ms | 2.6x | **2.4x** |
| Q3_low_sel | alice | 713,392 | 341ms | 15,024ms | 1,096ms | 44.1x | **3.2x** |
| Q4_projection | alice | 729,732 | 207ms | 10,777ms | 867ms | 52.0x | **4.2x** |
| Q5_compound | alice | 138,064 | 359ms | 3,035ms | 888ms | 8.5x | **2.5x** |
| Q1_scan_all | bob | 2,234,632 | 238ms | 46,948ms | 1,549ms | 197.4x | **6.5x** |
| Q2_high_sel | bob | 151,891 | 287ms | 3,297ms | 833ms | 11.5x | **2.9x** |
| Q3_low_sel | bob | 2,157,883 | 276ms | 44,569ms | 1,552ms | 161.2x | **5.6x** |
| Q4_projection | bob | 2,234,632 | 195ms | 32,786ms | 1,112ms | 168.1x | **5.7x** |
| Q5_compound | bob | 436,972 | 331ms | 9,161ms | 946ms | 27.6x | **2.9x** |

结论：JSON 回传瓶颈确认并消除——大结果集减速从 161~197x 收敛到
5.6~6.5x（约 30 倍改善），全部场景收敛至 2.4~6.5x；剩余开销为 Remote 侧
Iceberg 执行 + Parquet 写出 + 跨机读取，与预期一致。原始数据：
/tmp/fgac-bench-phase2p/phase2p_results.json。

## 追加：高选择率场景 2.4x 差距归因（2026-09-14）

以 alice Q2_high_sel（VendorID=1 AND fare_amount>=50，36,149 行）为样本，
P1 与 P2P 全部 5 次重复的原始耗时高度稳定（P1: 355~387ms，P2P: 887~935ms），
p50 对比 380ms vs 921ms。按实测分段拆解 P2P 的 921ms：

| 分段 | 耗时 | 说明 |
|---|---:|---|
| P1 参照（E 侧全程） | 380ms | E local[4]，Iceberg scan+filter+count，同进程无协议开销 |
| submit 段（E→R→E） | 660ms | 其中授权 RTT 实测 3ms、HTTP 往返 1.4ms，均非瓶颈 |
| ↳ R 侧 scan+filter | ~420ms | R local[2] 探针实测热缓存 422~460ms，算力仅 P1 一半 |
| ↳ R 侧结果 Parquet 写出 | ~230ms（分摊） | 探针实测整写 616ms（2 文件提交 MinIO），对 3.6 万行此为固定成本 |
| read 段（E 跨机读结果） | 261ms | E 经 s3a 读回 36,149 行结果对象 |

三个结构性差异点（对照 P1 的 380ms）：

### 差异点 1：执行域算力差（scan 段 ~420ms vs P1 全程 380ms）

P1 与 P2P 对同一份数据做完全相同的逻辑计算（Iceberg scan → `VendorID=1`
行过滤 → `fare_amount>=50` 谓词），但计算发生的进程与资源配额不同：

| | P1 | P2P |
|---|---|---|
| 执行位置 | E（引擎节点） | R（Remote 数据节点） |
| Spark 并行度 | `local[4]`（4 核） | `local[2]`（2 核，`remote/app.py` `spark()` 硬编码） |
| 数据物理位置 | R 的 MinIO，跨机 s3a 读取 | R 本地 MinIO，127.0.0.1 读取 |
| 输入扫描量 | 296 万行全表（谓词无分区裁剪） | 同左 |

R 侧探针（`/tmp/r_cost_probe.py`，独立 spark-submit 进程）实测：冷缓存
4,550ms（JVM/AQE 预热），热缓存 **422~460ms**。也就是说 R 即使数据在本
地、免去了 P1 的跨机 s3a 读（这部分 P1 反而要付），scan+filter 仍比 P1
慢——两者输入输出一致，唯一变量就是 2 核 vs 4 核。归因方法：R 侧硬件与
E 同规格（128 核），若 `local[4]` 对齐，按核数线性外推 scan 段约 210~230ms，
该段差异即消失。

附带说明：P1 的 380ms 中其实包含了跨机读 296 万行原始数据的时间（约
100~150ms，千兆内网约 40MB/s 有效吞吐），而 P2P 的 scan 段不含这部分——
所以 R 侧 420ms 与 P1 380ms 的"算力差"实际比表面数字更大，约 420 vs
230~280ms 的纯计算部分。

### 差异点 2：两跳数据流 vs 一跳（写出 230ms 分摊 + 读回 261ms）

P1 的数据流终止于引擎内存：`scan → filter → count`，结果只有一行计数。
P2P 因为结果必须作为独立对象返回给 E（E 不持有受控数据访问权），数据流
变成三段式：

```
R: scan+filter → 写 Parquet 到 MinIO（结果物化）
   ↳ 探针实测 3.6 万行整写 616ms（冷 1,656ms）；含：
     - Spark 单任务生成 2 个 part 文件（local[2] 2 个 core）
     - Parquet footer + 元数据写入
     - MinIO multipart 提交（s3a commit protocol）
   ↳ 对 3.6 万行这是行数无关的固定成本：文件创建、目录标记、
     _SUCCESS 标记、MinIO 对象元数据一个都不能少
E: 经 s3a 读取结果对象（read 段 261ms）
   ↳ 含 Iceberg/Hadoop 文件列表发现、footer 解析、36,149 行数据传输
     （约 1.2MB 列存，传输本身 <10ms，大头是对象列表 + 首字节延迟）
```

两跳合计约 500ms，是 921ms 中占比最大（54%）的一块。**固定成本的特性
决定了行数越少相对劣化越重**：36,149 行摊 500ms 是 2.4x 的主因；223 万行
同样摊约 500~600ms，但基数变成 46,948ms→1,549ms 中的 32%（所以 6.5x
而非 197x 的收敛）。这也是"高选择率劣化差距较小"的完整解释——绝对延迟
921ms 是全部十个场景最低的，只是分母小。

### 差异点 3：协议固定开销（实测合计 <5ms，可忽略）

逐项实测（2026-09-14，curl 计时）：

| 项 | 实测 RTT |
|---|---:|
| Polaris `/v2/authorize`（R 本机） | 3.4ms |
| E→R HTTP 健康往返（跨机） | 1.4ms |
| uvicorn 请求解析（FastAPI/pydantic 校验） | <2ms（81 次请求日志无异常延迟） |

三轮数据交叉验证：P2（JSON 回传）与 P2P（Parquet 回传）在 alice Q2 上
分别是 988ms 与 921ms——两种回传方式协议段相同，差异 67ms 全部来自编码
路径，反推协议本身占比 ≤7%。**结论：FGAC 的授权/策略机制（本方案的安全
核心）不构成可测量的性能成本**；2.4x 的全部成本来自"结果必须经 R 物化
再被 E 取回"的架构性数据搬运，与策略执行无关。

优化路径与预期收益：

| 优化 | 针对分段 | 预期 |
|---|---|---|
| Remote `local[2]`→`local[4]`（与 P1 对齐） | scan 420ms→~210ms | 总延迟约 -200ms，减速比 2.4x→~1.9x |
| 小结果集阈值内联回传（行数 < N 用 inline，> N 用 Parquet） | 免除 230ms 写出 + 261ms 读回 | 高选择率场景减速比 →~1.4x |
| Remote 结果写本地盘 + E 侧 HTTP range 读（免 MinIO 提交） | 写出段 | 小结果集再省 100ms+ |
| 结果对象小文件合并（write 单文件）/Spark `repartition(1)` | 写出段 footer 数 | 约 -50ms |

综合后 2.4x 的理论收敛下限约 **1.4~1.8x**：授权与一次跨机传输是"数据不出
R"安全前提的固有成本，无法消除。

## 追加：为什么"物化 → 传输 → 重建"不可省略（2026-09-14）

对照 [`CURRENT_ARCHITECTURE_PERFORMANCE_ANALYSIS.md`](../../CURRENT_ARCHITECTURE_PERFORMANCE_ANALYSIS.md)
阅读。该步的存在不是实现选择，而是由信任边界的**数据流向约束**推导出的
必然结果。论证分三层：

### 1. 授权模型决定了结果只能由 Remote 交付

本架构的授权链条是：引擎提交**计划**（不含身份、不含路径）→ Polaris 根据
Bearer Token 决定该主体能看到什么 → Remote 按授权契约执行行过滤与掩码。
过滤之后的行仍然是受治理数据。此时它们身处 R 的执行内存里，而要回到 E 的
查询里，只有两种可能：

- **E 自己来 R 取**：需要 E 持有存储凭据或数据访问权——直接违反"普通引擎
  不能获得受控目录或原始存储凭据"的不变量，等于把信任边界挪回引擎侧，
  整个 FGAC 架构退化为装饰；
- **R 送过去**：结果经一个双方都信任的通道跨越边界。这条通道上无论用什么
  编码（JSON、Parquet、Arrow），"编码 → 跨网 → 解码"这个最小形态都无法
  去掉——它是两个互不信任内存空间的唯一合法桥梁。

所以问题从来不是"要不要这一步"，而是"这一步每个字节花多少钱"。

### 2. 三种回传编码是同一桥梁的三种过桥费

| 编码 | 过桥费构成 | 实测（bob Q1，223 万行） |
|---|---|---:|
| inline JSON | 行式文本：Driver `collect()` → Py4J → Python List → JSON 编码 → HTTP → 逐行解码 → `parallelize` 回填 | 46,948ms（197x） |
| 结果 Parquet | 列存物化：Spark 并行写出 → 对象存储提交 → E 侧分区化并行读取（天然多分区） | 1,549ms（6.5x） |
| Arrow Flight（未实现） | 零拷贝流式：无 `collect()`、无落盘、边收边算 | 理论下限 |

JSON 的灾难性在于**每行都要过一次桥**且经历最多 8 次数据副本（分析文档
§4.1 的副本清单）；Parquet 把过桥单位从"行"变成"文件"，列存压缩让 223 万
行只剩约 26MB，且 E 侧读取天然并行、保留分区信息——这是 Phase 2P 收敛的
两个独立来源（编码效率 + 分区恢复），后者此前未被单独指出。

### 3. 真正的终局不是让过桥变便宜，而是让过桥的行变少

即便 Arrow Flight 把每字节成本压到极限，跨界的仍是被策略放行后的**全部明细
行**。分析文档 §4.3 指出的下推深化才是根治手段：

```sql
SELECT d.department, SUM(o.amount) FROM orders o JOIN departments d ...  GROUP BY d.department
```

若 Aggregate/Join 不可下推，跨越边界的是策略放行后的所有订单明细；若能
下推，跨界的只是分组聚合后的少量结果行——**结果路径成本从 O(明细行数)
变为 O(结果行数)**，选择率挂钩问题被从源头消解。这也是文档 §7 路线图把
"下推深化"排在"结果协议"之后的含义：两者分别压缩过桥的**行数**与**单价**，
正交且需同时推进。

**对本报告数据的最终推论**：Phase 2P 剩余的 2.4~6.5x 中，约一半是桥梁的
固定通行费（物化提交 + 回读首字节，~500ms，Arrow Flight 可消除），另一半
是随行数的可变成本（列存编码 + 传输 + 重建，Arrow Flight 可再降一个量级）。
收敛到 1.4~1.8x 后，剩余部分就是 §3 第 1 层论证的架构价格——为"数据不出
R"这一安全性质支付的、不可再压缩的下限。

## 追加：与 CURRENT_ARCHITECTURE_PERFORMANCE_ANALYSIS.md 的交叉验证（2026-09-14）

本报告的三轮实测为该分析文档的劣化优先级清单提供了实证锚点，同时该文档
修正了本报告的两处测量口径：

**实测确认的劣化点**（括号内为文档 §3 的优先级序号）：

| 文档劣化点 | 本报告证据 |
|---|---|
| (1) inline JSON 全量返回 | Phase 2 的 2.6x~197x，斜率 ~21µs/行 |
| (6) Remote `local[2]` 并行度损失 | 差异点 1 探针：同计算 R 2 核 420ms vs P1 4 核折算 230~280ms |
| (4) 统计信息缺失 | Phase 2P read 段与行数弱相关（261→282ms），佐证列存统计未被引擎利用 |
| (2) 整文件下载 + 临时落盘 | 已被本次 Iceberg 直读改造消除（该两项不再出现在 Phase 2/2P 路径中） |

**文档对本报告的两处修正**：

1. **Adapter Core 中转未纳入测量**：本报告 Phase 2/2P 从 E 直连 Remote:8002
   （文档 §4.7），生产路径应经 Adapter Core:8004 多一次完整解析/重编码。
   因此本报告数字对生产路径**偏乐观**；下一轮复测应串入 Adapter Core。
2. **PG 插件不具备大结果测试条件**：PG 路径固定 1024 个 `FgacRow` 且无
   动态扩容（文档 §4.1），存在越界风险。本报告仅测 Spark 路径是正确的
   范围界定，PG 侧性能结论应待插件修复后单独测得。

**路线图定位**：本次 Iceberg 直读 + Parquet 物化完成了文档 §7 的阶段一
（消除重复搬运）与阶段二的一半（列存结果编码）；尚未完成的是阶段二的
流式化（Arrow Flight）、阶段三的下推深化与 Cost Envelope 注入、阶段四的
并发治理。文档 §8 的指标清单（两节点 CPU、Shuffle 字节、首批延迟等）应
作为下一轮测试的采集基线。