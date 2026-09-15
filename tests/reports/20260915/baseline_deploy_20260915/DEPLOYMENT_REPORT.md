# Polaris 统一元数据及 Ranger 原生基线部署验证

验证日期：2026-09-15。范围：独立 `fgac-lab` 实验环境；旧部署保持运行。本报告是功能可用性验证，不是性能复测报告。

## 结果

新环境已完成 R/E 角色交换，并使用真正的 Apache Polaris 统一管理 Iceberg Catalog。Spark 已从 HadoopCatalog 切换为 Polaris REST Catalog。Ranger Admin 与 Kyuubi AuthZ 已连通，并通过行过滤、列脱敏、拒绝访问及会话内策略更新测试。

原先名为 `polaris-minimal` 的自定义服务提供 FGAC 兼容接口，并不承担完整 Iceberg REST 元数据管理。新环境仍保留该兼容端点，但表的注册、加载和提交由 Apache Polaris 完成；不再另建 Hive Metastore，也不需要为 Ranger 再注册一套 Iceberg 表。Ranger 使用 Hive 服务定义描述数据库/表/列授权资源，这不等于部署 Hive Catalog。

## 角色和服务

| 节点 | 新角色及目录 | 服务 |
|---|---|---|
| SSH 10023，172.168.22.23 | R，`/data1/lyb/fgac-lab` | MinIO、Apache Polaris、PostgreSQL、FGAC 兼容端点 |
| SSH 10025，172.168.22.25 | E，`/home/lyb/fgac-lab` | Spark、Kyuubi AuthZ、Ranger Admin、Ranger PostgreSQL |

| 组件 | 版本/配置 | 地址 |
|---|---|---|
| Apache Polaris | 1.7.0，Java 21，relational-jdbc | `http://172.168.22.23:18182/api/catalog` |
| Polaris 健康检查 | 管理接口仅本机 | `http://127.0.0.1:18183/q/health`，R 节点 |
| Polaris PostgreSQL | 16，`polaris_schema`，8 张内部表 | R 本机 `15433` |
| MinIO | 沿用原部署二进制，独立数据目录与凭据 | `http://172.168.22.23:19100` |
| MinIO Console | 仅本机 | R 本机 `19101` |
| FGAC 兼容服务 | 原 MinimalPolaris 源码，仅修改监听端口 | R `18181` |
| Spark | 3.5.8，Java 11，`local[2]` | 按作业启动 |
| Iceberg Spark Runtime | 1.7.2，RESTCatalog + HadoopFileIO/S3A | Catalog 名称 `fgac` |
| Kyuubi AuthZ | `kyuubi-spark-authz-shaded_2.12-1.10.2.jar` | Spark 扩展 |
| Ranger Admin | 官方 2.5.0 WAR，Java 8，内嵌 Tomcat | E 本机 `16080` |
| Ranger PostgreSQL | 16，独立数据库与账号 | E 本机 `15432` |

Spark、Python 和既有 Iceberg/Hadoop JAR 只读复用 E 节点原安装目录，所有新配置、日志、临时文件和数据库均位于 lab 目录。Polaris 安装包已校验 Apache SHA-512；Ranger WAR 已与 Maven Central 发布的 SHA-1 核对。

## 元数据管理口径

- Polaris：命名空间、表注册、当前 metadata location、提交并发控制和 Catalog 级权限；其内部目录状态持久化到 PostgreSQL。
- MinIO：保存 Iceberg metadata JSON、manifest、Parquet 等对象；不把所有文件内容搬入 PostgreSQL。
- Ranger：管理访问策略；Kyuubi AuthZ 将行过滤、列脱敏与授权检查应用到 Spark 查询。
- 新 Spark 配置只使用 RESTCatalog。注册后不再通过 HadoopCatalog 写入这两张实验表，避免出现两个提交入口。
- 实验 Catalog 的 `default-base-location` 与 `allowedLocations` 均限定在 `fgac/warehouse`；保留 `s3a://` 旧路径兼容性。MinIO 使用 path-style 和显式 endpoint。
- 当前以静态存储凭据访问实验 MinIO，`stsUnavailable=true`，未启用 Polaris 凭据下发。Polaris 客户端认证与行列授权是不同层次，不能把 Catalog 权限当成行列脱敏。

## 验证证据

| 检查 | 实际结果 | 证据文件 |
|---|---|---|
| 现有出租车表注册 | 原快照 `6896028106384817739` 保留 | `polaris-registration.json` |
| Polaris 重启持久化 | 仅 GET 检查，未重新注册；两张表快照均保持 | `polaris-restart-validation.json` |
| Spark 跨机读取原数据 | 2,964,624 行；VendorID 1/2/6 分别为 729,732 / 2,234,632 / 260 | `polaris-metadata-validation.json` |
| 元数据写入链路 | Spark CREATE、INSERT、SELECT、DROP PURGE 独立探针表全部通过 | 同上 |
| Alice 行过滤及脱敏 | 仅 VendorID=1 的两行；卡号为 MASKED，NULL 保持 NULL | `authz-all.json` |
| Bob 完整读取 | 三行测试数据，卡号原值可见 | `authz-all.json` |
| 未授权用户 | Kyuubi AccessControlException 拒绝 SELECT | `authz-all.json` |
| 会话内策略更新 | 无需重启 Spark，约 7.259 秒观察到 VendorID=2；随后恢复原策略并验证 | `ranger-policy-refresh.json` |

测试表 `fgac.baseline.authz_fixture` 内容：

| VendorID | card | amount |
|---|---|---|
| 1 | 12345678 | 10.0 |
| 2 | 87654321 | 20.0 |
| 1 | NULL | 30.0 |

权限测试原始 SQL：`SELECT VendorID, card, amount FROM fgac.baseline.authz_fixture ORDER BY amount`。没有在测试 SQL 中手工加入行过滤条件或脱敏表达式。

首轮出现“脱敏生效但行过滤未生效”；同时 Java 17 下 Ranger 内置脚本条件报告 Nashorn 缺失。最终固定为 Java 11，并对行过滤使用明确数据库和表名，重新跑通全部检查。由于两项配置共同调整，本报告不将初次失败归因于单一因素。后续 benchmark 不应未经验证就把行过滤策略改成通配资源。

## 运行方式

在对应节点：

```sh
python3 <LAB_ROOT>/deploy/labctl.py status
python3 <LAB_ROOT>/deploy/labctl.py start
python3 <LAB_ROOT>/deploy/labctl.py stop
```

也可指定单个服务，例如在 R 执行 `python3 /data1/lyb/fgac-lab/deploy/labctl.py start polaris`。管理脚本核验 PID 对应进程属于本 lab，避免误操作旧进程。服务没有注册系统开机自启；机器重启后先启动 R，再启动 E。

E 节点提交实验作业：

```sh
/home/lyb/fgac-lab/deploy/spark-lab.sh /path/to/job.py
```

重新验证权限：`python3 /home/lyb/fgac-lab/deploy/run_validation.py`。

Ranger UI 仅监听 E 本机。可使用 SSH 本地转发访问管理界面。凭据保存在 lab 的 `deploy/*-credentials.json` 和受限配置文件，不能提交 Git 或放入报告。当前保留测试 harness 的管理权限，不能向不可信用户开放这些配置文件。

## 隔离与空间

旧 Catalog PID 80220、Remote Python PID 837124、Remote Spark PID 838595、MinIO PID 4153454 的启动时间与部署前一致；旧 Catalog、Remote、MinIO 健康接口均返回 HTTP 200。

临时 HTTP 传输服务已关闭，已完成下载的重复分片已清理。收尾时 R lab 约 1.9 GiB，所在数据盘使用率 60%、剩余约 2.9 TiB；E lab 约 1.6 GiB，所在系统盘使用率 77%、剩余约 241 GiB。保留官方包、运行时和验证证据用于复现。

## 结论边界与后续基线

本次部署的是 Spark 内的 Kyuubi AuthZ 插件，没有部署完整 Kyuubi JDBC/Thrift 服务。测试通过受控的 `HADOOP_USER_NAME` 构造身份，不代表已完成生产身份认证或防止任意 Spark/Python 代码绕过；实际基线测试必须固定可信提交入口和相同授权假设。Ranger 审计写入日志，未部署 Solr 审计搜索。

Ranger 策略轮询间隔为 5 秒，实际观察到的 7.259 秒包括轮询、查询和观测开销，不是刷新时延 SLA。基线采用原生策略缓存，不是每条 SQL 在线向 Ranger 拉取策略。后续比较应统一 Polaris、数据快照、硬件、返回率、结果消费、预热及缓存口径，并单独注明各方案授权缓存机制。

本轮没有测量性能劣化，也没有验证 Spark 之外的引擎。现有旧系统没有自动切换为新的 Polaris Catalog；统一入口已在本次新实验环境落地。

官方参考：[Kyuubi AuthZ 安装](https://kyuubi.readthedocs.io/en/v1.10.2/security/authorization/spark/install.html)、[Polaris JDBC 持久化](https://polaris.apache.org/releases/1.7.0/metastores/relational-jdbc/)、[Polaris MinIO Catalog](https://polaris.apache.org/releases/1.7.0/getting-started/creating-a-catalog/s3/catalog-minio/)。
