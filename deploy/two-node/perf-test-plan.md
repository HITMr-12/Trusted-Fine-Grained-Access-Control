# FGAC 性能测试设计（Spark 两节点：E + 172.168.22.25）

> 初步设计文档。目标：在同一 Spark 引擎、同一 Parquet 数据快照下，对比
> **不带 Remote FGAC**（Phase 1 基线）与**带 Remote FGAC**（Phase 2）的性能差异，
> 量化细粒度访问控制功能的开销。

## 1. 拓扑与角色

```text
引擎节点 E（当前机器）                       远程/数据节点 R（172.168.22.25）
──────────────────────────────              ──────────────────────────────
Spark 3.5.6 + Iceberg 运行时                Polaris FGAC 控制面（catalog/polaris-minimal）
FGAC Spark 插件（fgac-spark-extension.jar）  Storage 服务（storage/app.py，受控 Iceberg 表数据）
Adapter Core（adapter-core/app.py）         Remote FGAC 执行器（remote/app.py，Iceberg 读取）
Benchmark 驱动脚本                          MinIO（两阶段共用数据通道，承载 Iceberg 表）
                                            受控 Parquet 快照（Iceberg 表的数据文件）
```

- 两阶段使用**同一台 E、同一台 R、同一份 Iceberg 表快照**，不互换角色。
- Phase 1 为性能参考基线，不是安全部署；Phase 2 才是完整 FGAC 链路。

## 2. 数据访问通道设计（关键决策：方案 B，Iceberg 表）

数据以 **Iceberg 表**组织，数据文件为 Parquet，存放于 R 上的 MinIO（S3 API）。
两阶段读取同一张 Iceberg 表：

| 阶段 | 读取路径 | 说明 |
|---|---|---|
| Phase 1 基线 | Spark + Iceberg (E) --s3a--> MinIO (R:9000) | 裸引擎经 Iceberg scan 直读远程表，不经 FGAC 授权，SQL 内手写等价过滤 |
| Phase 2 FGAC | Spark 插件 (E) --> Adapter Core (E) --> Remote (R) --Iceberg/MinIO--> 表数据 | 受控子树下推，Remote 授权后在 R 本地以 Iceberg scan 执行 |

Iceberg 目录（catalog）选型：**HadoopCatalog（文件系统 catalog）**，表元数据
（metadata.json、manifest）与数据文件同放 MinIO warehouse 路径下。理由：

- `catalog/polaris-minimal` 是硬编码 fixture，无 Iceberg REST Catalog 能力
  （`/v1/catalog` 不存在），不能直接当 Iceberg REST catalog 用
- HadoopCatalog 无需额外 catalog 服务，E/R 两侧用同一 S3 warehouse 路径即可
  指向同一张表，避免引入 Nessie/JDBC 等新组件
- Polaris 控制面继续只承担 FGAC 授权/策略角色，与 Iceberg catalog 职责解耦，
  后续演进到 Polaris REST Catalog 不影响本测试的两阶段可比性

备选方案取舍：

- **NFS/SFTP 挂载**：需要修改系统级配置，违反"不影响其他用户"约束，放弃。
- **数据拷贝到 E 本地**：破坏两阶段"同存储位置"的可比性，放弃。
- **MinIO（选定）**：单二进制、用户态部署在 R 的家目录；Spark（E）与 Remote（R）
  都经 s3a 访问同一份 Iceberg 表，网络条件一致。

## 3. 环境与版本固定

### 节点 E（已确认）

| 项 | 值 |
|---|---|
| CPU / 内存 | 128 核 / 2 TB |
| 磁盘 | /dev/sda4，可用约 452 GB |
| OS | Linux 5.15 |
| Java | OpenJDK 17.0.20 |
| Python | 3.10.12 |
| 待装 | Spark 3.5.6 发行版、Maven 3.9+（编译 Spark 插件）、Adapter Core 依赖 |

Spark 3.5.6 兼容 Java 17，满足要求。

### 节点 R（172.168.22.25，部署时确认）

- SSH：lyb（用户级部署，不使用 root，不改 `/etc`、`/usr` 等全局配置）
- 需确认：CPU/内存/磁盘、Java 17+（Polaris minimal 需 `javac`）、Python 3.10+、
  对外网（pip/Spark/MinIO 下载）是否可达
- 部署方式：官方 `deploy/native/server-r/install.sh` 要求 root，**改用用户态等价
  流程**：venv 安装依赖，进程用 `systemd --user` 或 `nohup` 托管，全部落在
  `$HOME/fgac/` 下。

### 版本固定清单（写入实验清单）

Spark 3.5.6、iceberg-spark-runtime 3.5_2.12（1.6.x 或 1.7.x，部署时固定）、
JDK 版本、pyspark 3.5.6、pyiceberg + s3fs（Remote 侧 Iceberg 读取，若走 pyiceberg
路径）、fastapi 0.116.1、uvicorn 0.35.0、MinIO RELEASE 版本、Iceberg 表
metadata.json 位置、snapshot ID、数据文件数、总行数、Parquet 压缩编码。

## 4. 数据集与 Iceberg 表

采用仓库既有 fixture：NYC TLC `yellow_tripdata_2024-01.parquet`
（官方发布，约 296 万行，约 50 MB），策略列天然齐全：

- 行级过滤策略：`VendorID = 1`（alice）、`VendorID = 2`（bob）
- 谓词列：`fare_amount`、`trip_distance`、`payment_type`、`Airport_fee`

数据组织为 **Iceberg 表**，注册/建表在 MinIO 的 warehouse 路径
（如 `s3a://fgac/warehouse/nyc.taxi_trips`）：

1. R 上以 Spark（R 本地）或 pyiceberg 完成一次性建表导入：原始 Parquet →
   Iceberg 表（数据文件仍为 Parquet，含元数据树）
2. 记录 snapshot ID、数据文件数、总行数；两阶段校验 snapshot 一致
3. Phase 1 从 E 经 `spark.sql.extensions=IcebergSparkSessionExtensions` +
   HadoopCatalog（S3 warehouse 路径）做 Iceberg scan
4. Phase 2 Remote 在 R 本地对同一张表做 Iceberg scan

若初测规模过小、差异不显著，二期将同一表 rewrite 成多分区/多文件版本
（如 10 份）扩大扫描量，两阶段同步放大。

## 5. Phase 设计与公平性契约

### Phase 1：native 基线（无 FGAC）

- Spark 挂载 iceberg-spark-runtime，经 HadoopCatalog 对 MinIO 上的 Iceberg 表
  做 scan，不加载 FGAC 插件、不连 Adapter Core / Remote。
- Benchmark SQL 显式包含与 Phase 2 策略**等价**的过滤和投影：
  `SELECT VendorID, trip_distance, fare_amount, payment_type FROM nyc.taxi_trips WHERE VendorID = 1 AND <谓词>`。
- E 上需一次性配置 s3a 凭据（基线专用），Phase 2 开始前从引擎环境移除。

### Phase 2：Remote FGAC

- Spark 加载 `GovernedSparkExtension` 与插件 JAR；Adapter Core 常驻 E。
- 受控扫描 → Remote Plan → Adapter Core → R 节点 Remote → Polaris 再授权 →
  Remote 以 Iceberg scan 读取受控表 → 行过滤/投影 → 结果回注 Spark。
- E 不持有任何受控存储凭据；Remote 故障必须 fail closed。

**Remote 侧 Iceberg 读取改造（方案 B 引入的代码变更，需确认后实施）**：
当前 `remote/plan_compiler.py:129` 用 `spark.read.parquet(本地文件)` 读取，
`remote/app.py:60` 从 Storage 整文件下载到临时文件。Iceberg 化改造为：
Remote 的 SparkSession 配置 Iceberg 扩展 + HadoopCatalog + s3a 凭据，
`source_loader` 改为返回 Iceberg 表标识（`LOAD TABLE ...` / `spark.table`），
Storage 服务从"整文件 HTTP 下发"退化为可选组件（Remote 直接经 MinIO 读表）。
这消除了临时文件落盘和二次网络拷贝，属于对现有代码的实质修改——已按约定向
用户报备，确认后执行。

### 公平性契约（两阶段全部固定）

- 同一张 Iceberg 表、同一 snapshot ID、同一数据文件布局
- 引擎与依赖版本一致；Spark `--master local[N]` 与 JVM 参数一致
- 预热次数（2 次）、正式测量重复次数（5 次，取中位数与 p95）、冷/热缓存条件标注
- E 与 R 都经同一 MinIO 端点/网卡读数据（Phase 1 跨机读、Phase 2 R 本地读，
  网络差值是 FGAC 架构的组成部分，报告中分别呈现）
- **CPU 计时双机采集**：FGAC 把计算从 E 移到 R，只看 E 的 CPU 会低估真实成本；
  用 `/proc/stat`（用户态可读）在两机分别记录查询期间的 CPU 时间

## 6. 负载矩阵（初测简化版）

| 编号 | 类型 | SQL 模式 |
|---|---|---|
| Q1 | 扫描重 | 全列 SELECT，无谓词（仅合法 principal 的全可见域） |
| Q2 | 高选择率 | `fare_amount >= 50` |
| Q3 | 低选择率 | `fare_amount >= 5` |
| Q4 | 投影 | 4 列中取 2 列 |
| Q5 | 复合谓词 | `payment_type = 1 AND fare_amount >= 20` |

- 主体：alice（`VendorID=1`）、bob（`VendorID=2`），每条 SQL 套用对应行策略
- 并发：**1 和 4** 两档（初测简化，后续可扩 8/16）
- 边界约束：当前 Remote Plan 协议仅支持 governed scan / filter / project，
  **不得把聚合、Join 标注为远端执行**；本初测不包含 Join 用例

## 7. 测量指标

| 类别 | 指标 |
|---|---|
| 延迟 | 端到端 p50/p95/p99、首批返回延迟 |
| 吞吐 | 每秒结果行数、MB/s |
| 数据量 | Storage/MinIO 读出字节数、E↔R 网络字节（两阶段各自抓取）、返回 E 的序列化字节 |
| 计算成本 | E CPU 时间、R CPU 时间（分阶段记录）、峰值内存（E 与 R） |
| FGAC 特有 | Remote 请求数、授权耗时（Polaris /v2/authorize）、Adapter Core 转发耗时 |
| 正确性 | 结果行数、结果哈希（两阶段必须一致） |

采集手段：Spark EventLog / `time.perf_counter`、`ss -s` 与网卡计数器
（`/sys/class/net/*/statistics`）、`/proc/<pid>/stat` CPU 刻度差。

## 8. 执行流程

1. 环境准备：E 装 Spark + iceberg-spark-runtime + Maven；R 用户态部署
   MinIO、Polaris、Remote（含 Iceberg 读取改造）
2. 建表导入：R 上把 NYC Parquet 导入 Iceberg 表（MinIO warehouse），记录
   snapshot ID 与行数
3. Phase 1：E 配置 s3a + Iceberg → 预热 2 次 → 每条 SQL × 并发档位重复 5 次 →
   采集双机指标
4. 回收基线凭据：从 E 移除 s3a 配置
5. Phase 2：构建插件 JAR → 启动 Adapter Core → 同样预热/重复/采集
6. 汇总：输出 CSV/JSON 原始数据 + 对比报告（Phase2/Phase1 延迟比、CPU 总量比、
   网络放大倍数），给出结论：FGAC 开销集中在授权、网络传输还是远端执行

## 9. 已知限制（初测声明）

- Remote 侧结果仍为内联 JSON 返回，序列化开销大；结果解释时需注明这是
  **当前实现**的开销，非架构理论下限
- `catalog/polaris-minimal` 为硬编码 fixture，不承担 Iceberg REST Catalog 职责；
  本测试用 HadoopCatalog，Polaris 只做 FGAC 授权面
- 明文 HTTP + 静态令牌，仅限隔离验证网络
- 未含 TLS、掩码策略、聚合/Join 下推；这些留待后续版本
- E 的 Python 3.10 低于 native 文档要求的 3.11，Adapter Core（fastapi 系）
  实际兼容，若遇阻再用 conda 装 3.11（用户级）

## 10. 待确认事项

- [x] ~~基线数据通道~~ 已定：两阶段均经 MinIO 读同一 Iceberg 表（方案 B）
- [x] ~~Iceberg 化改造 remote 读取~~ 已定：改 `remote/app.py` +
  `remote/plan_compiler.py` 的 source_loader，Storage 组件退化为可选
- [ ] R 节点硬件规格与外网可达性
- [ ] R 上需开放 8181/8002/9000（防火墙是否可控）
- [ ] 初测数据规模是否接受（约 50 MB），或需准备放大版
- [ ] iceberg-spark-runtime 版本（1.6.x / 1.7.x）部署时联网拉取并固定
