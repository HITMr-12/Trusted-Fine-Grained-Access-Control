# 黑盒 Remote Scan 代价估计测试报告

## 结论

原型已实现三类估计器，并新增远端接口 `POST /v2/subplans/estimate`。接口在 Catalog 授权和策略应用之后估计结果规模，只返回代价信封，不返回受控行。公开数据实测中，1% 策略感知采样将 P90 Q-error 从固定估计的 **712.68**、启发式估计的 **7.22** 降至 **1.09**；以 10 MiB 为广播阈值时，Join 决策正确率从 **55.56%** 提升到 **100%**，平均估计耗时为 **116.47 ms**。

## 实现边界

- `fixed`：模拟 Remote Scan 使用统一默认行数（1000 行）。
- `heuristic`：模拟传统无统计信息回退，策略等值过滤取 10%，残余谓词取 1/3。
- `policy_sample`：在已经应用用户策略和查询谓词的数据流上做固定种子 1% Bernoulli 采样，返回点估计、95% 区间、估计字节数和耗时。
- 本轮不考虑 CVM；安全边界仍是现有 Remote 执行域。真实行数只在离线测试脚本中计算，远端接口不返回真实基数。

## 数据与工作负载

数据采用 NYC TLC 官方 `Yellow Taxi Trip Records 2024-01` Parquet，实际读取 **2,964,624 行**。策略用 `VendorID=1`（Alice）与 `VendorID=2`（Bob）构造两个不同的可见域；每个主体执行 9 个谓词，共 18 个查询，包括金额范围、距离范围、支付类型、机场费和相关合取谓词。

这组用例覆盖三类主要误差来源：策略选择率、数据倾斜/相关性、复合谓词独立性假设。每个方法与完全执行得到的真实行数比较，并以 32 字节逻辑行宽和 10 MiB 阈值模拟 broadcast/shuffle 二分类。

| 方法 | P50 Q-error | P90 Q-error | 最大 Q-error | 平均估计耗时 | Join 决策正确率 |
|---|---:|---:|---:|---:|---:|
| fixed | 192.02 | 712.68 | 2155.73 | 0.003 ms | 55.56% |
| heuristic | 2.44 | 7.22 | 21.84 | 0.002 ms | 55.56% |
| policy_sample | 1.05 | 1.09 | 1.12 | 116.47 ms | 100% |

## 为什么这些量化指标合理

1. 基数估计研究普遍使用 Q-error。CardBench 在 20 个真实数据库和数千查询上用 Q-error 比较估计器；Learned Cardinalities 也采用该指标。
2. VLDB 的综合评测指出 Q-error 不能单独代表计划质量，因此本测试增加 Join 决策正确率和执行/估计时间；这对应论文提出的 plan/end-to-end 评价方向。
3. 联邦查询实证研究同样强调：基数误差应同时用查询运行时间和联邦执行细粒度指标衡量。Remote Scan 属于相同的跨执行域优化问题。
4. Spark AQE 官方文档明确使用运行时统计重新优化，并可能将 sort-merge join 转为 broadcast join；所以用字节阈值验证 Join 决策具有直接工程依据。
5. NYC TLC 是官方发布的真实 Parquet，而非为了估计器人工构造的数据，保留了支付类型、距离、金额等字段间的真实倾斜与相关性。

## 完整复现步骤

1. 确认数据文件：`storage/data/governed/nyc_taxi/yellow_tripdata_2024-01.parquet`。
2. 在项目根目录执行：`powershell -ExecutionPolicy Bypass -File tests/run_remote_scan_cost_benchmark.ps1`。
3. 检查机器可读结果：`tests/reports/remote_scan_cost_results.csv` 与 `.json`。
4. 重复执行三次，报告中位数；改变 `--sample` 为 0.001、0.005、0.01、0.05，绘制估计时延—P90 Q-error 曲线。
5. 扩展验证应增加冷/热缓存、不同文件数、不同策略选择率、并发 1/8/32，以及把 cost envelope 接入 PG `CustomPath` 和 Spark `Statistics` 后比较真实 `EXPLAIN` 计划与端到端时间。

## 通过标准与有效性威胁

- 建议门槛：P90 Q-error ≤ 2、Join 决策正确率 ≥ 90%、P95 估计延迟 ≤ 300 ms。
- 当前单次本地运行满足上述门槛，但不能据此宣称生产性能：数据只有一个月、缓存处于单机环境、行宽为逻辑近似值、Join 选择是阈值模拟而非两个引擎的真实优化器决策。
- 下一阶段必须把估计值真正注入 PG/Spark 的计划节点，并报告计划、运行时间、网络字节和并发吞吐，才能证明系统级收益。

## 公开依据

- NYC TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Spark Adaptive Query Execution: https://spark.apache.org/docs/latest/sql-performance-tuning.html
- Cardinality Estimation in DBMS: A Comprehensive Benchmark Evaluation (PVLDB 2022): https://www.vldb.org/pvldb/vol15/p752-zhu.pdf
- CardBench (2024): https://arxiv.org/abs/2408.16170
- Learned Cardinalities (CIDR 2019): https://vldb.org/cidrdb/papers/2019/p101-kipf-cidr19.pdf
- An Empirical Evaluation of Cost-based Federated SPARQL Query Processing Engines (2021): https://arxiv.org/abs/2104.00984
