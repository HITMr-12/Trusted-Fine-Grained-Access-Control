# FGAC 可信 Demo

报告入口：[实验与复测报告目录](tests/reports/README.md)；当前 MASK 分支的[历史结论对照](tests/reports/20260918/mask_e2e_review/COMPARISON.md)与[独立端到端复测](tests/reports/20260918/mask_e2e_review/REVIEW.md)。

本仓库保存不含可信硬件依赖的 FGAC Demo。控制面基于 Apache Polaris 1.7.0；Remote
执行域与 Spark/PostgreSQL 适配插件均为独立发布组件，不并入 Polaris 进程或制品。

## 目录边界

- `catalog/polaris-minimal/`：可独立构建的最小 Polaris FGAC 兼容控制面。
- `catalog/polaris-extension/`：用于合入完整 Polaris 1.7.0 的正式资源与测试源码。
- `remote/`：独立 FGAC 子计划验证与执行服务。
- `plugins/spark/`：独立发布的 Spark Catalyst Extension。
- `plugins/postgres/`：独立发布的 PostgreSQL Planner Hook/CustomScan。
- `plugins/artifacts/`：本地构建时生成插件制品；该目录不进入 Git。
- `storage/data/public/`：可由普通引擎直接读取的数据。
- `storage/data/governed/`：只能由 Remote 读取的受控数据。
- `clients/`、`postgres/`、`tests/`：引擎接入与契约测试。
- `deploy/native/`：两台 Linux 物理机的 systemd 原生部署入口。
- `deploy/two-node/`：纯净基线与 Remote FGAC 的测试设计和可比性约束。
- `docker-compose.yml`：仅用于开发机功能验证，不用于物理机性能测试。

## 当前迁移状态

本地 Compose 使用 `catalog/polaris-minimal` 提供兼容的 `/v2/relations` 和
`/v2/authorize` 接口，能够完成 Spark、PostgreSQL、Remote、Catalog 和 Storage
之间的最小端到端验证。`catalog/polaris-extension` 保留合入完整 Polaris Server 的
资源和测试源码，但完整 Polaris 构建不属于最小 Demo 的启动前提。

Polaris 基线：

```text
tag:    apache-polaris-1.7.0
commit: 4ac2f059d1cce149453d0a5f1ff1dff980ec97cc
branch: codex/fgac-polaris
```

完整 Polaris 集成以该上游版本为基线，并在 Linux/macOS 中按以下方式验证：

```bash
cd <apache-polaris-1.7.0-source>
cp -R <this-repo>/catalog/polaris-extension/* .
./gradlew format compileAll
./gradlew check
```

## 目标发布物

```text
fgac-polaris:<version>               # 修改后的控制面
fgac-remote:<version>                # 独立 Remote 数据面
fgac-spark-extension.jar             # 独立 Spark 插件
fgac_pg.so                            # 独立 PostgreSQL 插件
```

## PostgreSQL 谓词执行不变量

PostgreSQL 插件对受控关系采用“保守下推 + 本地强制复核”：

1. 仅将 Remote 协议能够精确表达的基础比较、布尔组合与 NULL 判断序列化为
   `filter`；不支持或语义敏感的表达式保留为本地 residual qual。
2. 无论谓词是否下推，原始 `scan.plan.qual` 都保留，并由 PostgreSQL
   `ExecQual` 在 Remote 返回治理行后重新计算。
3. `EXPLAIN` 中的 `Remote Filter` 展示实际下推表达式，`Local Recheck: true`
   表示 PostgreSQL 仍承担最终语义复核。
4. OR/NOT 只有在全部子表达式均可安全转换时才下推；AND 可以下推安全子集，
   但完整 AND 仍在本地复核。

## PostgreSQL 计划边界与 DML 不变量

1. `plan.targetlist` 保留 PostgreSQL 规划器生成的语义输出及字段引用；Remote
   返回的固定物理行结构单独记录在 `custom_scan_tlist`，不得用重建列覆盖上层
   `GROUP BY`、排序或窗口算子依赖的计划元数据。
2. `FgacRemoteScan` 通过标准 `ExecScan` 完成本地谓词复核和语义投影；Remote
   读取只负责填充物理扫描槽。
3. 普通 PostgreSQL 引擎中的受控关系是只读的。任何引用受控关系的
   `INSERT`、`UPDATE`、`DELETE`、`MERGE`、修改型 CTE，以及将受控数据写入
   本地表的 DML 均在 Planner 入口统一返回 `insufficient_privilege`；直接作用于
   受控关系的 `COPY` 和 `TRUNCATE` 同样在 Utility 入口拒绝。
4. 非受控本地表的正常 DML 不受影响。

完整回归：

```powershell
docker compose build pg-plugin-builder
docker compose run --rm pg-plugin-builder
docker compose run --rm pg-tests
docker compose run --rm security-tests
```

`postgres/tests/pg_predicate_pushdown_test.sql` 覆盖比较、布尔组合、NULL、残余
表达式、混合下推、CTE、子查询、Join、聚合、自连接、参数化条件和身份策略组合。
`postgres/tests/pg_plan_contract_dml_test.sql` 覆盖受控表直接分组、窗口组合、掩码
分组，以及受控关系 DML/数据外流拒绝和普通本地表 DML。

## 双模式接入验证

Demo同时保留两种引擎接入方式：

- 插件模式：Spark Catalyst Extension或PostgreSQL CustomScan提取受控子树，调用
  `/v2/subplans`，再将Remote结果回注本地计划。
- 非插件模式：纯净引擎不加载FGAC插件，通过
  `/v2/virtual-tables/{relation}`读取Catalog治理后的JSON或CSV虚拟表，再执行本地
  Join、Aggregate等剩余计划。

非插件模式验证：

```powershell
docker compose up -d --build remote
docker compose run --rm pg-non-plugin-tests
docker compose run --rm spark-non-plugin
```

该兜底路径的安全前提保持不变：普通PG和Spark均不挂载
`storage/data/governed`，也不持有Storage Token；principal和行策略只能由Catalog
根据Bearer Token确定。虚拟表接口不会接受引擎自行提交的principal或policy。

## Parquet存算分离边界

- 持久化受控数据统一保存在独立Storage组件的
  `storage/data/governed/**/*.parquet`。
- Storage仅加入内部`data`网络，并要求`STORAGE_TOKEN`；PG、Spark只加入
  `control`网络，没有受控Parquet挂载和Storage凭据。
- Remote是唯一同时连接`control`和`data`网络的计算组件。它先从Catalog获取策略，
  再按`storage_object`读取Parquet，并在Remote Spark中完成策略执行。
- 插件模式返回原生结果节点；非插件模式由Virtual Table Gateway把治理结果编码为
  JSON/CSV关系行流。JSON/CSV只是传输编码，不是新的持久化数据副本；原始数据仍只
  存在于远程Parquet存储。
- 对大规模部署，关系行流可替换为Arrow Flight；需要中间结果落盘时，只允许Remote
  写入带TTL和查询级凭据的结果Parquet，普通引擎仍不能访问原始Parquet命名空间。
