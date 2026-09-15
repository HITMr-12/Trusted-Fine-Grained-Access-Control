# 当前 FGAC 与原生授权基线复测口径

范围：只比较当前最优流式 FGAC（常驻 R Spark、Arrow Flight、有界提前预取）、Spark + Kyuubi AuthZ + Ranger 原生授权、直接内嵌行条件的理想化参考。排除 JSON、结果 Parquet 落盘、旧流式和自建 PlanCompiler 无 Remote 基线。

按用户要求比较实际部署形态：不限制 FGAC 两侧总计算预算、不绑核、不设置 CPU 配额。三条路径的 E Spark 均 local[2]、2 GiB Driver；FGAC 另有 R Spark local[2]、2 GiB Driver。这是部署形态的性能比较，不代表总计算成本相同。原生基线与理想化参考共用相同 E Spark 并行、JVM、Iceberg、S3A 设置。

所有路径使用 Spark 3.5.8、Iceberg 1.7.2、Java 11.0.32，同一 Polaris 1.7.0 Catalog、同一 MinIO 仓库与表快照 6896028106384817739。表含 2,964,624 行。开始、结束及每 10 组核对当前快照。未使用 DataFrame persist/cache、结果缓存或预计算摘要。目录缓存统一为 30 秒；保留 Ranger 的正常策略缓存和轮询。

保留旧七场景的阈值及输出行数：Bob 的 VendorID=2 授权行共 2,234,632 条；trip_distance 阈值依次为 30.06、20.18、8.67、1.70、0.65，以及无业务过滤；Alice 的 VendorID=1 行权限之上执行 fare_amount >= 50。另加 whole_source_100，独立主体 bench_full 被显式授予所有行，返回 2,964,624 行。

Bob 的“100%”指授权集合全部返回，占原表约 75.4%；whole_source_100 才是原表 100%。所有场景返回四个原始数值字段 VendorID、trip_distance、fare_amount、payment_type。旧场景没有列掩码，本轮不将其称为脱敏性能基准。

原生路径的 SQL 只含业务条件，行权限由真实 Ranger Hive 服务定义及 Kyuubi AuthZ 注入。直接条件参考的 SQL 包含等价行权限，保留 Polaris 表加载和同样的存储读取，但没有授权插件。FGAC 保留两次 Catalog 授权复核、一次性 ticket、原编译器、原 Arrow 输出和 2 批/16 MiB 有界预取。

whole_source_100 所需的新策略是明确的 `row_filter: {op: all}`；部署适配层仅为这一已授权策略返回原 DataFrame。既有 eq 策略仍使用原编译器；没有全量直传 Parquet、缓存结果或绕过 Remote Spark 的特化路径。Arrow 接收端使用旧测试的同一源码，重新编译为 Java 11 字节码。

计时从构建 SQL DataFrame / Flight describe 之前开始，覆盖授权、计划重写、源表加载、预取、E 引擎准备、全部输出四列的双 xxhash64 摘要聚合，以及流式资源关闭。控制测试进程的本机 socket 往返不计入任何路径。行数、字段名称/类型、两个摘要逐组核对，不以 count-only 或空消费代替全列读取。

每个场景 5 组预热、30 组正式测量。在正式测量前，将各 E Spark 会话的预热查询数补齐至至少 40 次：Alice 原生会话额外 35 次、Bob 原生会话额外 10 次、全表原生会话额外 35 次。每次额外预热也校验行数、字段和双摘要。六种路径顺序每场景各出现五次，固定种子 915，场景间交错随机化。正式共 720 次查询，常规预热 120 次，额外会话预热 80 次。无失败样本剔除规则；出现结果不一致或执行失败则整轮标记无效。

先前仅按场景预热的第一轮保留在 initial-* 证据中，但不作为最终主结论。其原生身份会话仅分别预热 5/30/5 次，而共享 FGAC/INLINE 会话各 40 次，存在 JVM 预热不均衡。修正的是测试工具口径，没有变更 FGAC 算法或根据结果选择不同实现。

报告独立中位数、P95、每组 FGAC/native 比值的均值与 95% bootstrap 区间；汇总旧七场景与新增八场景两套结果，并提供总耗时比和三个连续十组区块。理想化参考不构成安全等价方案，也不是数学上保证每次最快的严格上界。

部署目录：R `/data1/lyb/fgac-lab/deploy/fgac-current`，E `/home/lyb/fgac-lab/deploy/fgac-current`。新 Flight 端点为 `grpc://172.168.22.23:18815`，独立 FGAC 测试授权端点为 `http://172.168.22.23:18184`。既有 lab 18181 接口和旧部署均不修改。
