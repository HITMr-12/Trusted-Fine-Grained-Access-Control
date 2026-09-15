# 高返回比例下的转码与进程通信分析

基于 2026-09-15 两场景各 60 组正式复测、保存的 Spark 事件及实际部署源码。只分析既有证据，没有修改或重启服务。

## 结论与证据边界

现有证据支持：FGAC 增加了 R 端行转 Arrow、分区结果收集以及两次本机 JVM/Python 通信和跨机 Flight 链路；E 端存在明显等待上游供数的现象。不能把约 13.62% / 16.68% 的总体劣化全部归为 E 端转码或网络。

直接可测的 E Python IPC write_batch + flush 仅约 34–35 ms，远小于其取批次等待约 572–600 ms。后者包含上游生产、收集、通信、线程调度及缓冲影响，不是纯网络时间。当前未单独测量 E Java ArrowStreamReader.loadNextBatch，不能用 Python 写耗时代表整个 E 接收成本。

## 实际链路

1. MinIO 的 Parquet 经 Iceberg/S3A 进入 R Spark JVM。
2. R 执行计划包含 BatchScan → ColumnarToRow → Filter → Project。此处是源列式数据转 Spark 行执行；Parquet 解码本身也是原生基线的共同工作，不能全部计为 FGAC 新增成本。
3. collectAsArrowToPython 调用 toArrowBatchRdd，将执行结果行编码为 Arrow 批次；其 runJob 使用 it.toArray 收集每个分区的 Arrow 批次，再由回调写给 Python。这是分区粒度聚集，并非全表 collect，也非每产生一批就立即出分区。
4. R JVM 通过本机 socket 向 R Python 发 Arrow IPC；Python 使用 ArrowCollectSerializer 读取 RecordBatch。没有转成 pandas 或 Python 行列表。
5. R Python 的 GeneratorStream 通过 Flight 把批次发往 E Python。此处保留 Arrow 列式表示，但跨进程/网络封装、缓冲和复制仍有成本，不能由此声称端到端零拷贝。
6. E Python 预取线程将 RecordBatch 放入有界队列；pump 线程轮流向两个本机 socket 写 Arrow IPC，逐批 flush。
7. E JVM 使用 ArrowStreamReader.loadNextBatch 读入 Arrow 向量，由 ArrowColumnVector 包装成 Spark ColumnarBatch，再执行统一双摘要和聚合。适配器本身没有逐行构造 Spark Row；后续计算的执行方式不能仅由适配器推断为全程列式。

R/E 都为 local[2]，本次 Spark task 与 driver 在各自本机 JVM 内执行；不要把逻辑 task → driver 收集误写成另一段跨机 executor 网络通信。确实存在的独立进程边界是 R JVM→R Python、R Python→E Python（跨机）、E Python→E JVM。

Spark 分区收集及行转 Arrow 的实现已核对 [Spark 3.5.8 Dataset.scala](https://github.com/apache/spark/blob/v3.5.8/sql/core/src/main/scala/org/apache/spark/sql/Dataset.scala#L3988)。对应本地入口为仓库 streaming/spark_batches.py 的 iter_arrow_batches；接收路径为上一轮部署源码 flight_bridge.py 和 ArrowSocketSource.java。

## 本轮探针数值

下表为每项独立中位数；这些区间大量重叠，不构成可相加的耗时分解。

| 指标 | 90% 返回 | 授权集合 100% |
|---|---:|---:|
| 原生总耗时 | 854.9 ms | 896.4 ms |
| FGAC 总耗时 | 967.1 ms | 1043.6 ms |
| FGAC 首批到达 E，距查询开始 | 475.9 ms | 502.1 ms |
| 首批后至查询结束，每次相减后取中位数 | 487.5 ms | 539.4 ms |
| E pump 等待预取队列，累计 | 572.4 ms | 600.3 ms |
| E pump 写 Arrow IPC + flush，累计 | 33.8 ms | 35.1 ms |
| R batches 迭代器生命周期 | 692.7 ms | 723.6 ms |
| Arrow 逻辑数据量 | 56.33 MB | 62.57 MB |
| Arrow 批次数 | 248 | 275 |
| 源输入字节 | 8.49 MB | 8.30 MB |

首批时间来自 E 预取线程；队列等待来自 pump，包含首批等待的一部分。因此“502 + 600 + 35 ms”没有正确的物理含义。R 生命周期包含 yield 后被下游阻塞的时间，也不能当成独立扫描/编码 CPU 时间。

源输入是压缩列数据的 Spark 读取统计，Arrow 是解码后的四列逻辑字节。100% 情况约 7.54 倍，说明传输表示比压缩 Parquet 大；不是实测网卡流量倍率。

## 能定位到什么程度

### R 源输出与分区收集是主要调查对象

首批到 E 约 0.48–0.50 秒；Spark 源码显示存在分区结果收集屏障；两种源查询各有 3 个扫描任务，源记录均为 2,964,624。高返回比例时，每分区需要编码和收集更多结果后才能交付，存在明确的机制解释，但目前尚无分区内首批编码时间和收集完成时间的联合记录，不能把首批 500 ms 全归于该屏障。

队列上限为 2 批/16 MiB，但两场景队列峰值中位数均只有 1 批、229,376 字节；60 次中达到 2 批的查询分别为 14 次和 17 次。这不支持“持续卡在 16 MiB 队列上限”作为主因。队列仍可能短暂背压；峰值指标不能替代完整占用时间曲线。

### E 更明显的是等待，而非摘要计算 CPU 暴涨

100% 场景原生任务 run time 总和中位数约 1208.5 ms、任务 CPU 总和 1151.2 ms；E FGAC 分别约 1791.5 ms、989.4 ms。E FGAC 的 CPU 没有增加，任务占用时间却增加，支持等待输入/同步等非 CPU 时间增加的判断。两者 task 数分别为 4 和 3，不能视为严格相同任务的差分。

这些都是多任务累计指标，不是端到端墙钟时间；约 802 ms 的 run−CPU 差额是两项中位数的粗略差，不能称为实测网络等待。该差还可能包含 I/O、调度、GC、锁等待等。

### 转换与通信的总体计算成本确实增加

100% 场景 FGAC E+R Python/JVM 进程 CPU 合计中位数约 4334.6 ms，原生 E 约 1611.6 ms。这说明流水重叠虽减少墙钟开销，额外工作并未消失；该指标不能单独确定是 Arrow 编码、内存复制还是通信框架消耗。独立 Catalog、MinIO、Ranger CPU 不包含在这个合计里。FGAC 的额外资源保留为部署优势，不作为不公平条件。

无内存/磁盘 spill，不能把劣化解释成结果落盘。R 的 GC 不能完全排除：100% 场景任务 GC 累计中位数为 52 ms，90% 为 15 ms；任务 GC 指标可能重复覆盖同一 JVM GC 暂停，不应直接累加为查询额外耗时。

## 缺少的测量及下一步优先级

| 边界 | 需要补充 | 要回答的问题 |
|---|---|---|
| R Spark task | 每分区第一/最后 Arrow 批次时间、编码 CPU、输出字节 | 行转 Arrow 是否主耗时，分区屏障多长 |
| R JVM→Python | socket 写/读时间、首末批、真实 IPC 字节 | 本机 IPC 是否阻塞，哪里在复制或等待 |
| R Python/Flight→E Python | 每批读取、发送/到达时间，gRPC/网络字节，线程 CPU | 是生产不及时还是跨机链路限速 |
| E 预取队列 | 已有 read_ms/backpressure_ms 的逐查询导出及占用时间序列 | 队列是否真正持续背压 |
| E Python→JVM | 写入与 Java loadNextBatch 的每批耗时、线程 CPU、向量包装时间 | Arrow 解码/分配还是等待 socket |
| E Spark 消费 | 批次可用→消费结束时间 | 下游计算是否反压生产者 |

这些点应先用于少量隔离探针查询，再用无细粒度探针的正式测量确认，防止逐批日志自身改变性能。跨机时间线需核对时钟偏差；同机阶段优先使用单调时钟。不应据现有累计数据直接给出“转码占 X%、IPC 占 Y%”的精确比例。

本轮尚未导出预取对象现有的 read_ms/backpressure_ms，也没有 Java 读端耗时。最有价值的补点是 R 分区首批/完成边界和 E Java loadNextBatch，这能把当前约 600 ms 的混合等待进一步拆开。

详细数值见 pipeline-metrics.json；本轮 R 物理计划见 r-pipeline-plan.txt；原有复测结论及原始证据仍在 RECHECK_REPORT.md 和 r/e/formal 目录，未被本分析修改。
