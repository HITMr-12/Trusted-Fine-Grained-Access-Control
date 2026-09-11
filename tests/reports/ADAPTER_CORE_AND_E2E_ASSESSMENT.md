# 通用 Adapter Core 与端到端测试判定

## 已生成的最小架构

`adapter-core` 是引擎无关服务。PG/Spark Shim 继续负责识别计划节点和本地行格式，Core 复用 Remote 传输、统一计划转发、估计接口、execute-once、物化标识、主体隔离、TTL 与重扫计数。Remote 仍会向 Catalog 重新授权，Core 不成为授权者。

当前 PG 和 Spark 插件均通过 `adapter-core:8004/v2/subplans` 访问 Remote。PG 的 `CustomScanState` 已具备查询内一次加载、`ReScanCustomScan` 重置本地游标的语义；`EXPLAIN` 会显示一次远程执行、本地物化和非参数化属性。

## 哪些测试是端到端

| 测试 | 是否端到端 | 实际覆盖 |
|---|---|---|
| `remote_scan_cost_benchmark.py` | 否，估计器微基准 | 公共 Parquet → Spark DataFrame → estimator → Q-error/模拟 Join 决策 |
| `adapter_core_contract.py` | 组件集成测试 | Core → Remote → Catalog/Storage，以及 execute-once、重扫、主体隔离 |
| `pg-tests` | 是，功能端到端 | PG SQL/Planner → 动态插件 → Core → Remote → Catalog/Storage → PG 后续 Join/聚合 |
| `spark-engine` | 是，功能端到端 | Spark SQL/Catalyst → JAR 插件 → Core → Remote → Catalog/Storage → Spark 后续算子 |
| 估计影响优化器 | 尚不是端到端 | PG `CustomPath.rows/cost` 和 Spark `Statistics` 尚未消费 CostEnvelope |

目前可以证明通用 Core 没有破坏两个引擎的查询正确性，也能证明独立物化协议只远程执行一次；还不能证明估计器改善了 PG/Spark 的真实 Join 计划。下一项严格系统测试必须把 CostEnvelope 注入两个引擎，比较真实计划、Remote 调用次数和端到端时间。

## 116 ms 平均估计耗时的来源

`policy_sample` 调用 `df.sample(...).count()`。这是一个真正的 Spark action：每个查询都会启动 Spark job、调度分区、读取缓存列批、执行策略与查询谓词并归并 count。Bernoulli sample 虽然只保留约 1% 的行，仍必须检查输入行才能决定是否保留。因此 116 ms 主要是 Spark 调度与扫描 CPU，不是置信区间计算，也不是 HTTP/Catalog 延迟。

测试使用 `raw.cache()`，所以该数字是热缓存、单进程条件下的估计动作时间；不包含网络下载、Catalog 授权、Remote HTTP 或冷 Parquet I/O。它不能称为端到端估计延迟。

生产方案不应为每个候选计划启动采样 Job，而应维护带版本的 footer/NDV/直方图/采样概要，由 Core 缓存 CostEnvelope；只在低置信度且接近 Join 阈值时异步采样，并用实际执行反馈更新缓存。规划阶段应以读取缓存为主。

## 已执行结果

- Adapter Core 契约：远端物化调用 1 次，重复 materialize 命中；本地 rescan 1 次；Bob 读取 Alice 结果返回 403。
- PG 全量功能测试通过。
- Spark 插件 Join/聚合用例通过。
- Core 计数观察到 PG/Spark 经统一入口执行了 38 次请求。
