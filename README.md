# Trusted-Fine-Grained-Access-Control

本仓库实现并验证了一套**不依赖可信硬件**的细粒度访问控制（FGAC，Fine-Grained
Access Control）架构：当 Spark 这样的普通分析引擎需要查询受治理的数据时，引擎
本身拿不到数据、也拿不到存储凭据——受控扫描被截获并转发到一个独立的远程执行域，
在那里完成授权、行级过滤和列掩码之后，只把"治理后的结果"交还给引擎。控制面、
数据面、引擎适配三层彼此独立发布，任何一层被攻破都不会直接泄漏原始数据。

## 它解决什么问题

传统的数据权限方案里，引擎（Spark/PostgreSQL）拿着数据副本或存储凭据在本地
执行过滤。这意味着引擎进程被攻破等同于数据泄漏，策略绕过（比如直接读底层
文件）也难以防御。本仓库把信任边界挪了位置：

```
Spark / PostgreSQL（普通引擎，无凭据）
    │
    ├── 普通对象 ──→ Catalog 授权 ──→ 公开数据，直接读
    │
    └── 受控子树 ──→ Remote 执行域（唯一持有存储凭据的组件）
                          │  1. 向 Polaris 控制面二次授权
                          │  2. 按主体策略执行行过滤 / 列掩码
                          │  3. 治理后的结果回传引擎
                          ▼
                    受控 Parquet / Iceberg 表（数据不出此域）
```

关键不变量：引擎全程只提交"我想查什么"的计划，从不提交身份或策略；身份由
Bearer Token 在控制面确定；原始数据、原始 Parquet 命名空间对引擎不可见；
Remote 失败时引擎查询失败（fail closed），不存在绕过路径。

## 仓库结构

```
catalog/polaris-minimal/     最小 Polaris FGAC 控制面（演示用 fixture，
                             硬编码 alice/bob 两个主体与行策略）
catalog/polaris-extension/   合入完整 Apache Polaris 1.7.0 的正式资源与测试源码
remote/                      Remote 数据面：接收子计划、向控制面授权、
                             在本地执行行过滤/掩码、物化结果
adapter-core/                引擎侧适配核心：代理与缓存 Remote 调用
plugins/spark/               Spark Catalyst Extension（截获受控子树、生成
                             Remote Plan、回注结果）
plugins/postgres/            PostgreSQL Planner Hook / CustomScan 适配
                             （保守下推 + ExecQual 本地强制复核）
storage/                     受控数据的存储服务（Demo 用整文件 HTTP 下发）
clients/                     Spark/PostgreSQL 接入演示客户端
postgres/                    PG 插件测试 SQL（谓词下推、DML 拒绝等契约）
tests/                       契约测试、性能测试脚本与测试报告
deploy/                      部署入口（见下节）
docker-compose.yml           开发机功能验证用，不用于物理机性能测试
```

### deploy/ 下的部署文档

- `deploy/native/`——两台 Linux 物理机的 systemd 原生部署（正式路径），
  含安装、构建、清理脚本；
- `deploy/cluster/`——容器化集群验证（仅开发验证用）；
- `deploy/two-node/`——两节点 benchmark 的设计契约与本次性能测试方案
  （`perf-test-plan.md`）。

## 双模式引擎接入

- **插件模式**：Spark Catalyst Extension 或 PostgreSQL CustomScan 截获受控
  子树，提交 `/v2/subplans`，把 Remote 结果回注本地计划。PostgreSQL 侧对
  下推采取"保守下推 + 本地强制复核"不变量（详见下文不变量章节）。
- **非插件模式**：纯净引擎不加载 FGAC 插件，经 `/v2/virtual-tables/{relation}`
  读取 Catalog 治理后的虚拟表。安全前提不变：引擎不挂载受控存储、不持有
  Storage Token，principal 与行策略只能由 Catalog 根据 Bearer Token 确定。

## Parquet 存算分离边界

- 持久化受控数据统一保存在独立 Storage 组件；Storage 仅加入内部 `data`
  网络并要求 `STORAGE_TOKEN`；普通引擎只加入 `control` 网络，无受控数据
  挂载与凭据。
- Remote 是唯一同时连接 `control` 与 `data` 网络的计算组件：先从 Catalog
  获取策略，再按 `storage_object` 读取数据，并在 Remote Spark 中完成策略
  执行。
- 结果回传支持两种编码：内联 JSON 行流（小结果集）与带 TTL + 查询级凭据的
  结果 Parquet（大结果集，见性能测试 Phase 2P）；普通引擎始终不能访问原始
  Parquet 命名空间。

## 性能测试：方案与结果

### 测试要回答的问题

同一引擎、同一份数据，**带 FGAC 链路比裸引擎直读慢多少？慢在哪一段？**

### 方案

完整设计见 [`deploy/two-node/perf-test-plan.md`](deploy/two-node/perf-test-plan.md)，
要点：

- 两节点拓扑：**节点 E**（引擎节点：Spark 3.5.8 + Iceberg 1.7.2）与
  **节点 R**（172.168.22.25：MinIO + Polaris 控制面 + Remote 执行域），
  均为 ARM64，128 核 / 2TB 内存；
- 数据：NYC TLC 2024-01 出租车行程（官方数据，296 万行，48MB），以
  Iceberg 表 `fgac.nyc.taxi_trips` 存放于 R 的 MinIO；
- 行级策略映射到 `VendorID` 列：alice 可见 `VendorID=1`（73 万行），
  bob 可见 `VendorID=2`（223 万行）；
- 三轮同负载对比（5 条 SQL × 2 主体 × 预热 2 次 / 重复 5 次取 p50）：
  - **Phase 1**：裸 Spark 经 s3a 跨机 Iceberg scan，SQL 内手写等价过滤——
    性能参照；
  - **Phase 2**：受控扫描经 `/v2/subplans` 下推到 R，Remote 授权并执行，
    结果以内联 JSON 回传——原始实现；
  - **Phase 2P**：同上，但结果物化为 Parquet 对象（查询级凭据 + TTL），
    E 经 s3a 读回——回传路径优化后。

### 结果

完整数据与归因见
[`tests/reports/PHASE1_VS_PHASE2_RESULT.md`](tests/reports/PHASE1_VS_PHASE2_RESULT.md)，
原始测量数据在测试机 `/tmp/fgac-bench-phase{1,2,2p}/` 下。摘要（p50 延迟）：

| 场景 | 行数 | Phase 1 | Phase 2 (JSON) | Phase 2P (Parquet) |
|---|---:|---:|---:|---:|
| 高选择率（Q2） | 3.6 万 | 380ms | 988ms（2.6x） | 921ms（2.4x） |
| 复合谓词（Q5） | 13.8 万 | 359ms | 3,035ms（8.5x） | 888ms（2.5x） |
| 全表扫描（Q1） | 223 万 | 238ms | 46,948ms（197x） | 1,549ms（6.5x） |

三轮测试讲了一个完整的优化故事：

1. **Phase 2 的 2.6x~197x**：开销与返回行数强相关。归因实测确认瓶颈不在
   FGAC 授权（授权 RTT 仅 3ms），而在 Remote 把结果逐行编码成 JSON 文本
   回传——223 万行时序列化/传输/解析占绝对主导。
2. **Phase 2P 收敛到 2.4x~6.5x**：结果改为 Parquet 物化 + 查询级凭据回读
   后，大结果集约 30 倍改善，且开销不再随行数剧烈恶化。这验证了本 README
   所述"结果 Parquet + TTL + 查询级凭据"演进路线的有效性。
3. **剩余差距的构成**（高选择率 2.4x 的逐段归因，含 R 侧独立探针数据）：
   执行域并行度差（Remote `local[2]` vs 基线 `local[4]`）、结果物化 +
   回读的两跳固定 I/O（约 500ms，行数无关）、协议开销（<5ms，可忽略）。
   理论收敛下限约 1.4~1.8x——授权与一次跨机传输是"数据不出 R"这一安全
   前提的固有成本。

### 复现

```bash
# Phase 1（E 节点，需先按 perf-test-plan.md 部署 R 节点服务）
spark-submit ... tests/phase1_baseline_benchmark.py --out /tmp/fgac-bench-phase1
# Phase 2 / Phase 2P
python3 tests/phase2_fgac_benchmark.py  --out /tmp/fgac-bench-phase2
python3 tests/phase2p_fgac_benchmark.py --out /tmp/fgac-bench-phase2p
```

## 不变量与安全边界（摘要）

### PostgreSQL 谓词执行

1. 仅将 Remote 协议能精确表达的基础比较、布尔组合与 NULL 判断序列化为
   `filter`；语义敏感的表达式保留为本地 residual qual。
2. 无论谓词是否下推，原始 `scan.plan.qual` 都保留，由 PostgreSQL `ExecQual`
   在 Remote 返回治理行后重新计算；`Local Recheck: true` 表示 PG 承担最终
   语义复核。
3. 受控关系在普通 PG 引擎中只读：引用受控关系的 DML/修改型 CTE 在 Planner
   入口返回 `insufficient_privilege`；直接作用于受控关系的 `COPY`/`TRUNCATE`
   在 Utility 入口拒绝。非受控本地表的正常 DML 不受影响。

### Remote Plan 协议

- 当前仅支持 governed scan / filter / project 三种算子下推；聚合与 Join
  不应标注为远端执行，直至协议与编译器实际支持。
- Remote 对子计划做白名单校验（`ALLOWED_RELATIONS`、算子、表达式形状、
  深度限制），策略执行在授权契约返回之后、结果返回之前。

## 目标发布物

```text
fgac-polaris:<version>               # 修改后的控制面
fgac-remote:<version>                # 独立 Remote 数据面
fgac-spark-extension.jar             # 独立 Spark 插件
fgac_pg.so                           # 独立 PostgreSQL 插件
```

## 当前状态与边界

- 本次两节点性能测试的全部代码改动（Remote 的 Iceberg 读取与 Parquet 结果
  物化、Polaris 契约映射 NYC 列）尚未提交，见 `git status`；
- `catalog/polaris-minimal` 是演示 fixture，不承担真实认证与策略持久化；
  完整集成以 `catalog/polaris-extension` + Apache Polaris 1.7.0 为基线：
  将扩展目录拷入上游源码后 `./gradlew format compileAll` + `./gradlew check`；
- 通信为明文 HTTP + 静态令牌，仅适用于隔离验证网络；生产化前需补 TLS、
  密钥轮换与审计；
- 后续方向：Arrow Flight 流式回传（对比 Parquet 物化的上限）、Remote 并行度
  对齐、小结果集阈值内联、并发档位扩展（8/16）。

## 相关文档

- 两节点 benchmark 设计契约：`deploy/two-node/README.md`
- 原生部署与清理：`deploy/native/README.md`、`deploy/native/CLEANUP.md`
- 历史评估报告：`tests/reports/`（Adapter Core 评估、代价估计器报告）
- 完整 Polaris 集成基线：tag `apache-polaris-1.7.0`，
  commit `4ac2f059d1cce149453d0a5f1ff1dff980ec97cc`
