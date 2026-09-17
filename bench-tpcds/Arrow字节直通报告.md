# Arrow 字节直通（方案 A 一期）实测报告

日期：2026-09-17 ｜ 分支：bench-tpcds-0917 ｜ 结论：**工程目标达成（6 跳→3 跳、digest 全等），性能判负（墙钟 1.13× 于 FGAC 基线）——E 端物化税已被流水线掩盖，墙钟杠杆在传输量而非编码跳数。**

---

## 1. 结论速览

| 维度 | 结果 |
|---|---|
| 正确性 | digest 与 NATIVE 参考**逐位全等**（rows=28,800,991；sum1=-1375359135572699178175；sum2=-41844483081394350909571） |
| 墙钟（bench_full_1_0，100% 选择率） | FGAC 基线中位 **13,390 ms** vs FGAC-RAW 中位 **15,045 ms** → **1.13× 劣化** |
| E 端 CPU | 28.8 s（FGAC） vs 30.2 s（RAW），**无下降** |
| R 端生产耗时 | 12.8 s（comp/Flight） vs 14.8 s（araw 双流失重） |
| 传输尾差 | 两者 E 端 action 仅比 R 端生产多 0.3~0.5 s（传输几乎完全重叠） |

**判负依据**：字节直通的上限就是"与 FGAC 打平"——它消除的 E-1/E-2 两跳（Flight 反序列化 + bridge 再序列化，合计约 4.2 s CPU）本来就与 R 端生产等待（next_wait ≈ 8.4 s）**重叠执行**，从未出现在墙钟关键路径上；而它在 R 端新增的"写临时 IPC 文件 + 轮询读回 + 8MB 分块"使 R 端生产变慢 ~2 s，直接进墙钟。

---

## 2. 背景与设计

Arrow 通路原有 6 跳序列化链（税模型实测 3.8 ms/MB 的来源）：

```
R-1 Spark IPC 序列化(引擎原生) → R-2 Python 反序列化 → R-3 Flight 再序列化
→ 线上 → E-1 Flight 反序列化 → E-2 bridge 再序列化 → E-3 Java 反序列化(引擎原生)
```

方案 A 一期目标：E 端改纯字节管道，消 E-1/E-2，6 跳→3 跳。R 端 `serve_remote_tpcds_araw.py`（端口 18839）把 Spark 批次写成**两条完整独立的 Arrow IPC 字节流**（偶/奇批次交替），按 8MB 分块、首字节 tagged 交错发送；E 端 `arrow_raw_bridge_tpcds.py` 按首字节路由、原样写入两条本地 socket，**零解析、零 RecordBatch 物化**；Java `ArrowSocketSource` 每 socket 一个 reader，引擎内只做一次解码（E-3 保留）。

**设计修正历程**（两条死路已排除）：
1. 单流版：digest 全等对，但 22.5 s ≫ FGAC 14.2 s——单 socket 使 Java 侧并行读损失，证伪。
2. E 端按 IPC 消息边界拆双流：落盘验证发现流前缀长度只覆盖 metadata、body 紧跟其后，边界假设错误，证伪。最终改为 R 端产两条自带 schema 头的完整 IPC 流，E 端无解析。

---

## 3. 实测数据

### 3.1 同场 A/B（2026-09-17 下午，同机同数据，各 warmup 1 + 5 reps）

| 模式 | total med (ms) | total min | action med | E端 e_cpu med |
|---|---|---|---|---|
| FGAC（18833, Flight 基线） | 13,390 | 13,198 | 13,291 | 28.8 s |
| FGAC-RAW（18839, 字节直通） | 15,045 | 14,937 | 15,044 | 30.2 s |

RAW 各 rep：15,506 / 15,534 / 15,045 / 14,937 / 15,404。早先同会话验证跑为 14,504/14,551/13,984——两次合看为 **parity ~ 1.13× 劣化**，无双流修正前单流版的 22.5 s。

### 3.2 墙钟分解（R 端阶段遥测，同窗口）

| R 端路径 | 生产耗时（wall） | R 端 CPU |
|---|---|---|
| comp → Flight（FGAC） | 12.8 s（batches 3,536 / 28.8M 行） | 58.5 s |
| araw 双流写文件（RAW） | 14.8 s（ipc_write 14.2 s，首帧 1.2 s） | 66~81 s |

关键观察：FGAC 的 E 端 bridge_ipc_write 4.2 s + next_wait 8.4 s ≈ 12.6 s，恰好等于 R 端生产 12.8 s——**E 端物化与 R 端生产全程重叠**；而 RAW 的 E 端 action 15.0 s ≈ R 端生产 14.8 s + 0.2 s 尾差，传输几乎不占墙钟。

### 3.3 wire 量

两种模式线上量几乎相同（1,512 MB vs 1,513 MB，RAW 每帧多 1 字节流号 + 帧头）。字节直通不改变传输量。

---

## 4. 判负分析：为什么"少 2 跳"没有变快

1. **被消除的税不在关键路径上**。3.8 ms/MB 的税模型量的是 CPU；FGAC 通路上 E 端每收一批就做 ipc_write，与等待下一批（next_wait）交替，总墙钟 = max(R 生产, E 消费) + 尾差。E 消费速率（≈ R 生产 + 4 s CPU 摊到 12.8 s 上）从未反超 R 生产，故 CPU 税从未显性化。
2. **R 端新增成本直接进墙钟**。araw 为拿到"可直通的字节"要先把 IPC 流写盘再轮询读回（8MB 分块、5ms 轮询），R 端 CPU 从 58.5 s 涨到 66~81 s，wall +2 s。即使改为内存队列消掉落盘，上限也只是追回这 2 s 回到 parity——**编码跳数对称**（两侧都是 Spark IPC 编一次 + 发送路径再编一次），字节直通无结构性优势。
3. **E-3 引擎内解码消不掉**（除非 Parquet 直通等更深层方案），E 端 CPU 自然无下降。

## 5. 对本项目路线图的修正

- **性能墙的坐标确认**：100% 选择率下墙钟 ≈ R 端 Spark 扫描（12.8 s）+ 不可重叠尾差；1.5 GB 传输本身在千兆级内网上被流水线完全吸收。因此唯一有效的墙钟杠杆是**砍传输量/砍 R 端扫描量**，与"编码/解码次数最低"原则一致但落点不同：最低次数的达成不应靠 E 端字节管道，而应靠**让需要编码的数据变少**。
- **已判负清单**（本分支累计）：分帧重编码（R 端 write 15.8 s > FGAC 14.2 s 下限）、并行度扩容（local[4]/[8] 零收益）、字节直通一期（本报告）。三个负结果共同指向：R 端单机扫描与 Python 物化是硬地板。
- **下一步（先验知识方向）**：登记权限时已知 filter（如 Region=US），可做 **L1 策略感知布局**——建仓/重排时按过滤列聚簇，使 R 端文件级/Pages 级剪枝后**待编码数据量直接随选择率下降**（复活此前 Page 粒度短路 + 直通混合通路的价值），这是唯一同时降低"扫描量、编码量、传输量"三者的机制。

## 6. 复现

- R：`serve_remote_tpcds_araw.py`（FGAC_ARAW_PORT=18839，双流版）；基线 `serve_remote_tpcds_comp.py`（18833 NONE profile）。
- E：`arrow_raw_bridge_tpcds.py`（2-stream 路由版）+ `tpcds_worker.py` FGAC-RAW 分支（ArrowSocketSource，2 socket）。
- 验证：`python3 par_probe.py <outdir> 18839 5 FGAC-RAW`；A/B：同参数打 18833（FGAC）。
- 原始记录：R `runs/tpcds-bench/remote-stages.jsonl`（kind=araw / kind=stream）；E `runs/ab-0917-fgac/par-18833.jsonl`、`runs/ab-0917-raw/par-18839.jsonl`、`runs/araw-validate/`。
