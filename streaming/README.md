# Arrow Flight 流式结果原型

独立运行的 Remote 数据出口。保留现有 Catalog 授权和 `PlanCompiler`，将结果按 Arrow RecordBatch 发送，不调用 `materialize_result`，不生成结果 Parquet，也不先构造完整结果列表。

本目录是新增模块，不修改已有 HTTP Remote、Catalog 或部署脚本。当前接入方式是 Python 客户端 SDK；Spark、Daft、Trino 的正式连接器尚未实现。

## 请求流程

1. 客户端以 Authorization header 传入身份，用 `GetFlightInfo` 提交原有 v2 结构化计划。
2. 独立 Remote 实际请求 Catalog，复用部署版编译器应用行策略、掩码和业务条件。返回 schema 与一次性 opaque ticket，不执行结果物化。
3. `DoGet` 再次核对身份和当前策略，消费 ticket，返回数据批次。
4. 客户端迭代 Flight reader。空结果正常结束；生产者错误作为失败返回，不当作完整结果；退出扫描上下文会取消读取。

原型的 Flight 元数据端点充当扫描入口适配层。**尚未修改 Catalog，使它原生返回 Flight 入口。** Catalog 仍是权威策略来源，数据不经过 Catalog。

## 启动

将 `streaming/` 放在可导入的模块路径上。使用独立虚拟环境或独立 `--target` 依赖目录安装 `requirements.txt`，不要在现有生产环境中直接升级依赖。

服务端需要部署原本使用的 Spark/PySpark、FastAPI、requests 等依赖，以及 PyArrow 19.0.1。`--service-root` 必须指向具有 `fetch_source` 和 Iceberg 读取能力的部署源码；当前本地旧版 Remote 与该部署版本不相同，不能不加核对地替换。

```bash
python -m streaming.server \
  --service-root /path/to/deployed/repository \
  --location grpc://127.0.0.1:8815 \
  --batch-rows 8192 \
  --audit /path/to/private/stream-audit.jsonl
```

通过环境变量提供与已有服务相同的 `POLARIS_URL`、`FGAC_ICEBERG_TABLE`、Spark/jars 和 S3A 设置。在 Linux 隔离验证时，也可使用 `--env-from-pid <原服务PID>`，只读取原进程环境到新进程内，不改原进程、不将环境写入日志。PID 必须重新核对，不能把历史测试 PID 当作长期配置。

默认只监听回环地址。示例 `grpc://` 不含 TLS，仅用于受控内网验证；跨越可信网络边界时需要配置 TLS，当前命令行尚未暴露证书参数。令牌放在 header 中，不放入虚拟地址或 descriptor。

启动会创建自己的 Spark 会话，不复用或修改生产 Spark 会话。进程接收 SIGTERM/SIGINT 后关闭 Flight 服务并停止自己的 Spark。

## 客户端

```python
from streaming.client import GovernedClient, iter_batches

plan = {
    "version": 2,
    "schema_version": "1",
    "root": {
        "op": "project",
        "columns": ["VendorID", "trip_distance", "fare_amount", "payment_type"],
        "input": {"op": "governed_scan", "relation": "lake.sales.orders"},
    },
}

client = GovernedClient("grpc://remote-host:8815", token_from_your_session)
try:
    with client.scan(plan) as (schema, reader):
        for batch in iter_batches(reader):
            consume(batch)  # 消费或交给执行引擎，不将所有批次存进列表
finally:
    client.close()
```

### 在消费引擎准备期间预取

如果引擎需要先创建读取任务，可在取得 schema 后立即开始有界预取，让 Remote
扫描与引擎准备重叠。该优化不修改服务端协议，不需要更换查询或授权策略。
`describe()` 仍只取得元数据；调用 `prefetch()` 才消费 ticket 并执行原有 DoGet 复核。

```python
info = client.describe(plan)
with client.prefetch(info, max_batches=2, max_bytes=16 * 1024 * 1024) as batches:
    prepare_consumer(info.schema)
    for batch in batches:
        consume(batch)
```

必须在消费端准备或读取失败时也退出上下文，以取消预取。预取不会自动重试一次性
ticket；生产者错误会在迭代时传播，不能把失败前已收到的部分数据视为完整成功。
原有 `scan()` 的返回值与行为保持不变，客户端需主动采用上述提前启动方式。
只在确认即将执行查询时启用；不要在纯 schema 探测或可能永远不执行的懒惰计划
构造阶段自动预取。消费端准备失败会增加少量已启动的扫描工作，应及时取消。

队列默认最多保留两个批次，并设置 16 MiB 排队预算。为兼容大字段，一个超预算
批次可以独占空队列；生产线程也可能持有一个尚未入队的批次。该上限不包含 Flight
或 Spark 内部缓存，不代表全链路内存上限。不改变批次内容、类型或顺序。

统一性能复测须从 `describe()` 之前开始计时，覆盖预取、引擎准备、完整消费及关闭，
不能先预取数据再启动计时。

## 实现边界

- 支持已有编译器的无序 scan/filter/project 计划；不支持排序、聚合、Join 下推。
- 默认每批最多 8192 行，单个服务只允许一个活跃扫描；最多保留 64 个待消费 ticket。ticket 绑定令牌摘要，默认 300 秒租约，一次性使用，读取前重新检查策略指纹。租约在批次边界检查，暂不支持续租或自动重试。
- `spark_batches.py` 使用 Spark 3.5 的私有 `collectAsArrowToPython` socket 和 `ArrowCollectSerializer`，直接迭代其中的 RecordBatch，跳过末尾的批次排序索引。没有调用 PySpark 的 `_collect_as_arrow()`，因为后者会先建立完整批次列表。该适配器必须随 Spark 版本验证，不能视为稳定公共 API。
- Spark 内部仍可能按分区缓存任务结果，数据经过独立 Spark Driver；**不能据此声称已实现大规模分布式流式扫描或全链路严格内存有界**。这一轮按需求只实现基本流式方案。
- 当前 schema 映射支持常见标量及 decimal；时间戳、嵌套类型等尚未适配，遇到不支持的类型会拒绝。结果顺序未定义，与当前无 ORDER BY 的查询一致。
- 当前不压缩 Arrow 数据。传输量可能明显大于压缩 Parquet。统一消费口径的性能比较已保存在工作区报告中，收益取决于场景与客户端接入方式。
- 客户端取消会关闭 socket 迭代并取消本次 Spark job group。它不提供任意批次续传或持久结果缓存。
- ticket 只保存进程内扫描状态，不持久化结果。已经建立的 DataFrame 用于本次读取；尚未实现可跨实例重放的快照扫描计划。

## 验证

传输单元测试（不需要 Spark 或 Catalog）：

```bash
python -m unittest streaming.test_streaming -v
```

覆盖身份缺失/非法、元数据不触发数据生产、分批/空结果、ticket 所有者和单次使用、过期、策略变化、部分流失败，以及提前预取、队列背压、超预算单批、字符串/空值、阻塞读取和队列满时取消。

两机实际验证的明细保存在工作区 `output/streaming_prototype_20260914/`。验证客户端逐批计算与既有 Spark 相同的双 XXH64 摘要；它没有把结果写到磁盘或收集为整张表。该 Python 校验器的耗时不是之前 Spark 端到端性能基准的替代数据。

参考：[Flight 协议](https://arrow.apache.org/docs/format/Flight.html)、[Python GeneratorStream](https://arrow.apache.org/docs/python/generated/pyarrow.flight.GeneratorStream.html)。
