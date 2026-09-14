# 当前 FGAC 架构性能劣化分析与存储路径优化方案

## 1. 报告范围

本文分析当前 `demo_0911` 架构中，Spark/PostgreSQL 热加载插件接管受控对象扫描后可能产生的性能劣化。分析以现有实现为准，覆盖：

- 引擎插件识别受控对象和拆分计划；
- Catalog 发现与 Remote 二次授权；
- Adapter Core 转发；
- Remote Spark 读取 Parquet并执行策略；
- 治理结果返回及本地计划继续执行；
- 复杂查询中的基数估计、Join和聚合。

本文不讨论 CVM带来的额外开销，也不把已有估计器微基准结果等同于端到端性能结果。

## 2. 当前执行架构

```text
Engine node E                                      Remote/data node R
------------------------------------               ---------------------------
Stock Spark / PostgreSQL                           Minimal Polaris FGAC
        |                                          Storage service
        | Hot-loaded plugin                        Remote Spark local[2]
        v                                                 |
Adapter Core :8004                                      Parquet
        |
        +---------------- HTTP -------------------------->
```

受控查询的主要执行路径为：

```text
SQL
  -> 引擎原生分析/优化器
  -> 插件发现受控关系
  -> Catalog对象发现
  -> 生成Remote Plan
  -> Adapter Core
  -> Remote
  -> Catalog二次授权及策略绑定
  -> Storage HTTP下载完整Parquet
  -> Remote临时文件
  -> Remote Spark扫描、过滤和掩码
  -> collect到Remote Driver
  -> inline JSON
  -> Adapter Core解析并重新编码JSON
  -> 引擎插件解析并本地物化
  -> 本地Join、聚合等剩余算子
```

因此，一次查询的端到端耗时可以近似拆分为：

```text
T_total =
    T_plan_catalog
  + T_adapter_proxy
  + T_remote_authorize
  + T_full_parquet_transfer
  + T_temp_file_io
  + T_remote_spark
  + T_collect_and_encode
  + T_result_network
  + T_plugin_decode_and_materialize
  + T_local_residual
```

## 3. 核心判断

当前最主要的性能劣化并不是 AST遍历或计划序列化，而是远程扫描之后的数据移动、集中式物化和优化器信息缺失。

| 优先级 | 劣化点 | 主要影响 | 性质 |
|---:|---|---|---|
| 1 | 治理结果以内联JSON全量返回 | 内存、CPU、网络、首批延迟 | 当前Demo实现问题 |
| 2 | 每次查询完整下载Parquet并临时落盘 | 固定全文件I/O、重复复制 | 当前Remote/Storage实现问题 |
| 3 | 仅下推Scan、Filter、Project | 大量中间数据返回引擎 | 当前下推能力不足 |
| 4 | Remote Scan统计信息错误或缺失 | 错误Join顺序和Broadcast决策 | 优化器集成不足 |
| 5 | PostgreSQL规划阶段同步访问Catalog | 多表查询规划延迟 | PG插件实现问题 |
| 6 | Remote仅使用`local[2]`，Spark回填最多两个分区 | 并发争用、并行度丢失 | 当前执行配置问题 |
| 7 | Adapter Core全量解析并重新编码响应 | 额外一次HTTP与内存复制 | 当前适配层实现问题 |
| 8 | Catalyst规则遍历和Remote Plan序列化 | 规划CPU和少量对象分配 | 插件固有但通常较低 |

## 4. 主要性能劣化

### 4.1 全量结果使用 `collect()` 和 inline JSON

Remote执行完成后调用`governed_df.collect()`，把全部结果集中到Remote Spark Driver，再转换成Python List并由FastAPI编码成JSON。相关实现见[`remote/app.py`](../../remote/app.py)。

数据可能经历以下副本：

```text
Spark Executor Row
  -> JVM Driver Row
  -> Py4J/Python Row
  -> Python List
  -> JSON响应缓冲区
  -> Adapter Core Python对象
  -> 再编码JSON
  -> 引擎插件响应缓冲区
  -> Spark InternalRow / PostgreSQL Tuple
```

主要后果：

- Remote必须在返回第一行之前完成整个Spark Action；
- Remote Driver必须容纳完整结果；
- JSON相较Arrow等二进制列式格式具有更高的体积和编码成本；
- Adapter Core再次解析并编码完整响应；
- 引擎无法边接收边执行；
- 大结果会同时增加R节点和E节点的内存压力。

Spark插件还会把所有JSON行存入`ArrayBuffer`，再通过`parallelize`生成最多两个本地分区。相关实现见[`GovernedRemoteExec.scala`](../../plugins/spark/src/main/scala/demo/fgac/GovernedRemoteExec.scala)。这会丢失原始文件分区、排序和分桶信息，并增加下游Shuffle。

PostgreSQL插件当前固定分配1024个`FgacRow`且没有动态扩容。相关实现见[`fgac_pg.c`](../../plugins/postgres/fgac_pg.c)。返回超过1024行时，这不仅是性能问题，还可能造成越界写入，因此当前PG路径不适合直接进行大结果性能测试。

### 4.2 每次请求完整下载Parquet并写临时文件

当前Remote通过Storage HTTP接口获得文件，使用`response.content`把整个Parquet放入Python内存，再写入临时文件供Spark读取。查询结束后临时文件被删除。

```text
R节点原始Parquet
  -> Storage进程
  -> HTTP/loopback
  -> Remote Python整文件内存
  -> 临时Parquet文件
  -> Spark Parquet Reader
```

即使查询只投影一列并返回一行，完整对象仍会被搬运。当前Storage和Remote同在R节点，因此该路径没有带来物理隔离收益，却增加了：

- 一次完整源文件读取；
- 一份整文件Python内存副本；
- 一次完整临时文件写入；
- Spark对临时文件的再次读取；
- 并发查询对同一对象的重复搬运；
- 临时空间容量和清理压力。

该问题可能掩盖真正的策略执行及插件成本。

### 4.3 下推算子范围不足

Remote Plan当前仅支持：

```text
governed_scan
filter
project
```

Aggregate、Join、Sort、Limit、Top-N、Window和Distinct等算子不能进入Remote。以受控订单表与公开部门表Join为例：

```sql
SELECT d.department, SUM(o.amount) AS total
FROM orders AS o
JOIN departments AS d ON o.owner = d.owner
GROUP BY d.department;
```

当前大致执行为：

```text
Remote: Scan orders -> 行策略 -> 掩码 -> 返回治理后明细
Local : Join departments -> Group By -> SUM
```

如果策略后仍有大量明细，但最终聚合只有少量行，系统仍需将全部明细通过JSON返回E节点。因此当前实现更适合高选择性过滤和小结果查询，不适合低选择性策略、大范围明细、Join和聚合密集型查询。

### 4.4 Remote Scan统计信息没有进入优化器

Spark的`GovernedRelation`和`GovernedRemoteSubplan`目前均报告`sizeInBytes = 1`。见[`GovernedPlans.scala`](../../plugins/spark/src/main/scala/demo/fgac/GovernedPlans.scala)。Catalyst可能因此错误地选择Broadcast Hash Join或不合理的Join顺序。

PostgreSQL的`CustomPath`使用本地占位关系的`rel->rows`，并设置固定形式的启动和总成本。它没有反映：

- 用户行策略选择率；
- 查询谓词选择率；
- Remote执行成本；
- 网络和反序列化字节；
- 首批数据延迟。

项目已经实现策略感知Cost Envelope，但尚未注入Spark `Statistics`或PG `CustomPath`。现有1%采样测试的平均估计耗时约为116 ms；该结果来自热缓存微基准，不包含Catalog、HTTP、Parquet下载和端到端执行，不能直接作为系统开销结论。见[`REMOTE_SCAN_COST_TEST_REPORT.md`](REMOTE_SCAN_COST_TEST_REPORT.md)。

### 4.5 PostgreSQL Catalog发现没有缓存和连接复用

PG在规划受控关系时同步调用Catalog。当前HTTP实现会重复进行DNS解析、新建TCP连接，并显式使用`Connection: close`。多表、子查询和CTE会放大规划延迟。

```text
T_pg_plan ~= T_native_plan + N_relation_lookup * T_catalog_round_trip
```

Spark也在分析阶段同步访问Catalog，但当前实现具有表名级`TrieMap`缓存，因此通常主要影响首次访问。相关实现见[`ResolveGovernedRelations.scala`](../../plugins/spark/src/main/scala/demo/fgac/ResolveGovernedRelations.scala)。

### 4.6 Remote并发和分区能力不足

Remote Spark当前固定为`local[2]`。多个用户共享Remote时，所有请求竞争同一个SparkSession和两个本地执行线程。同时，每个请求还会重复下载对象、启动Spark Action并收集结果。

因此单并发测试不能代表系统的并发能力。高并发下需要重点观察：

- Spark Job排队；
- R节点CPU和磁盘争用；
- Remote Driver堆内存和Python内存；
- 临时文件容量；
- p95/p99首批与完成延迟；
- 不同用户任务之间的公平性。

### 4.7 Adapter Core增加一次完整中转

当前插件访问Adapter Core，Core再调用Remote。Core使用`requests.post(...).json()`解析整个响应，然后由FastAPI重新编码。见[`adapter-core/app.py`](../../adapter-core/app.py)。

相较插件直接访问Remote，这增加了一次HTTP处理、一次完整JSON解析、一次完整JSON编码和一份结果对象。该成本对小结果可能较低，但随结果大小线性增长。

Adapter Core已经提供execute-once物化接口，但Spark和PG当前主执行路径仍直接使用`/v2/subplans`代理接口，尚未充分利用查询级共享物化能力。

## 5. 插件自身开销与架构开销的区分

### 5.1 Spark插件直接开销

- Catalyst计划遍历；
- 首次Catalog发现；
- 安全表达式判定；
- Catalyst表达式序列化为Remote Plan；
- JSON结果转换为`InternalRow`。

其中计划遍历和小型计划序列化通常不是主要瓶颈。同步Catalog调用、结果反序列化和集中式回填更加重要。

### 5.2 PostgreSQL插件直接开销

- Planner Hook和CustomPath构造；
- 每个关系的同步Catalog请求；
- 谓词表达式序列化；
- 整个HTTP响应缓冲；
- 基于字符串扫描的JSON行解析；
- 每行构造PostgreSQL Datum。

PG插件当前最主要的直接开销是Catalog连接没有复用，以及结果协议和本地物化方式不适合大数据。

### 5.3 当前Demo架构开销

- 完整Parquet HTTP下载；
- 临时文件写入及二次读取；
- Remote `collect()`；
- 多层JSON编码和解析；
- Adapter Core全量代理；
- Remote低并行度；
- 受限下推导致大量明细回传。

这些成本不是“热加载插件必然导致”的，而是当前原型协议与实现选择导致的。

## 6. 第二项问题的重点解决方案：Remote原生读取受控存储

### 6.1 目标架构

Storage不再通过HTTP传输整个文件，而负责对象定位、版本约束或凭证发放。Remote在授权后使用Spark原生文件系统连接器读取Parquet。

```text
Engine提交relation_id
  -> Remote请求Catalog授权
  -> Catalog返回storage_object、版本、策略和允许算子
  -> Remote在受信映射中解析物理位置
  -> Spark直接读取Parquet
  -> 列裁剪、Row Group裁剪、谓词下推
  -> FGAC策略执行
```

引擎不得提交物理路径，只能提交逻辑关系和计划。物理位置必须由Catalog授权结果和Remote受信配置共同确定。

### 6.2 当前双节点Demo的最小改造

由于Storage和Remote同在R节点，推荐让Remote服务账户以只读方式直接访问：

```text
/srv/fgac/data/governed/orders/
```

Catalog仍返回逻辑对象：

```json
{
  "relation_id": "lake.sales.orders",
  "storage_object": "governed.orders.v1",
  "schema_version": "1",
  "object_version": "2026-09-11",
  "content_hash": "sha256:...",
  "policy_version": "2"
}
```

Remote内部解析：

```text
governed.orders.v1
  -> file:///srv/fgac/data/governed/orders/
```

Spark随后直接读取：

```python
spark.read.parquet("file:///srv/fgac/data/governed/orders/")
```

普通Spark/PG进程不持有该目录的文件权限，也不获得受控存储凭证。

### 6.3 对象存储环境

如果数据位于S3、OBS、HDFS或Ceph，Catalog可以向Remote发放短期、最小权限的存储凭证，Remote Spark通过S3A等原生连接器读取。

这样可以利用：

- Parquet列裁剪；
- Footer和统计信息；
- Row Group跳过；
- Range GET；
- 并行分区读取；
- 连接池和重试；
- Spark原生调度机制。

存储凭证只发给Remote执行身份，不得进入普通引擎或Remote Plan。

### 6.4 必须保留Storage数据网关时

如果Remote不能直接持有存储位置或凭证，Storage需要从“整文件下载接口”演进为支持Seek/Range的读取服务，或者提供Hadoop `FileSystem`兼容连接器。

```http
HEAD /v1/objects/{object_id}
GET  /v1/objects/{object_id}?offset=...&length=...
```

Spark可先读取Parquet Footer，再请求必要的列块和Row Group。该方案需要额外实现连接池、重试、幂等、对象版本固定和请求级授权，因此不建议作为当前Demo的第一步。

### 6.5 不可变对象缓存

跨节点或对象存储场景可以增加内容寻址缓存：

```text
cache_key = storage_object + object_version + content_hash
```

缓存要求：

- 对象版本变化时使用新缓存键；
- 完整校验后原子发布；
- 仅Remote服务账户可读；
- 引擎不能指定缓存路径；
- 设置容量上限和LRU淘汰；
- 缓存目录纳入部署清理清单；
- 敏感数据缓存卷使用静态加密。

当前R节点本来就保存Parquet，因此不应再建立重复本地缓存，直接只读访问源文件即可。

### 6.6 必须保留的安全约束

消除Storage文件代理不能削弱授权边界：

1. 引擎只提交逻辑`relation_id`，不能提交路径或URI；
2. Remote必须向Catalog重新授权；
3. Catalog验证主体、关系、算子、列和Schema版本；
4. Remote根据授权结果解析`storage_object`；
5. 解析后的路径必须位于允许的数据根目录；
6. 执行期间固定对象版本，避免检查与使用之间的对象替换；
7. 普通引擎不具有受控存储权限；
8. 行策略和掩码只能来自Catalog授权契约；
9. Remote拒绝未经允许的算子、列和对象版本。

## 7. 推荐实施顺序

### 阶段一：消除重复文件搬运

- Storage增加对象描述接口，或由Polaris返回版本化对象描述；
- Remote根据`storage_object`解析本机只读路径；
- Spark直接读取`/srv/fgac/data`下的Parquet；
- 删除`response.content`、`NamedTemporaryFile`和查询后临时删除路径；
- 保持Catalog授权和Remote计划验证不变。

### 阶段二：替换结果协议

- 使用Arrow IPC、Arrow Flight或等价二进制协议；
- 分批返回结果；
- 本地引擎边接收边执行；
- 保留分区标识和Schema；
- 避免Remote Driver `collect()`。

### 阶段三：优化复杂查询

- 把Limit、Top-N、部分Aggregate作为优先新增下推算子；
- 根据代价决定受控侧Join是否进入Remote；
- 将Cost Envelope注入Spark和PG优化器；
- 避免错误Broadcast和Join顺序。

### 阶段四：并发与复用

- Remote改为独立Spark集群或多Executor模式；
- 增加查询级资源配额和公平调度；
- 统一execute-once和结果句柄；
- 避免同一查询重扫触发重复Remote执行；
- 增加对象元数据、Footer和Cost Envelope缓存。

## 8. 测试指标

优化前后应使用相同数据快照、资源总量、查询参数、并发和结果哈希，至少记录：

- 端到端p50、p95、p99延迟；
- 首批结果延迟和完成延迟；
- Storage读取字节；
- 临时文件写入字节；
- Remote扫描字节；
- R到E返回字节和序列化后字节；
- Remote、Adapter Core和Engine峰值内存；
- 两节点CPU时间；
- Spark Job数、Stage数和Shuffle字节；
- Remote调用次数；
- Catalog发现与授权次数；
- 不同用户并发1、4、8、16时的吞吐和尾延迟；
- 结果行数和结果哈希。

针对第二项优化，应首先验证：

```text
每查询Parquet临时写入字节 = 0
Remote Python整文件缓冲 = 0
Storage整文件HTTP请求 = 0
结果哈希与优化前一致
未经授权的引擎仍无法直接读取受控Parquet
```

## 9. 结论

当前架构确实存在显著的性能劣化，但主要成本不来自插件扫描执行计划，而来自完整Parquet复制、临时文件、Remote集中式`collect()`、inline JSON、多层物化、有限算子下推和错误统计信息。

对于当前两台物理服务器，优先级最高且改造风险最低的工作是让R节点上的Remote Spark在Catalog授权后直接只读访问本机受控Parquet。该改造能够消除每查询一次的整文件HTTP传输和临时文件I/O，但不能单独解决大结果返回问题。要得到可扩展的端到端性能，还必须继续将inline JSON替换为分区化、流式二进制结果协议。
