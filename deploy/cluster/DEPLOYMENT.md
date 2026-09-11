# FGAC 集群验证部署指南

## 1. 交付物

| 包 | 部署位置 | 内容 | 运行端口 |
|---|---|---|---|
| `fgac-remote-node-1.0.0.zip` | 独立 Linux 物理机 | Remote 镜像、执行代码、Compose、健康检查 | TCP 8002 |
| `fgac-control-storage-1.0.0.zip` | 本地 Docker 主机或物理机 | Polaris FGAC、Storage、Parquet 数据 | TCP 8181、8003 |
| `fgac-engine-adapters-1.0.0.zip` | 引擎主机 | Spark/PG 镜像、插件、初始化脚本和测试 SQL | PG TCP 5432（按需） |

镜像采用 `docker save` 导出，可完全离线加载。源码、配置和数据文件与镜像分离，便于替换配置而无需重建镜像。

## 2. 网络与信任边界

部署前分配两个固定地址：`CONTROL_HOST`（Catalog/Storage）和 `REMOTE_HOST`。推荐防火墙规则：

| 来源 | 目标 | 端口 | 用途 |
|---|---|---:|---|
| Remote | Control | 8181/TCP | 授权与对象元数据 |
| Remote | Storage | 8003/TCP | 使用服务令牌读取受控 Parquet |
| Spark/PG | Control | 8181/TCP | 发现受控关系 |
| Spark/PG | Remote | 8002/TCP | 提交受控子树 |
| 其他主机 | Storage | 8003/TCP | 应拒绝 |

当前 Demo 使用明文 HTTP 和静态令牌，只适用于隔离验证网络。跨主机验证至少应通过防火墙限制源地址；生产化前需增加 TLS、密钥轮换、身份认证和审计。

## 3. 前置条件

- Linux x86-64，或可运行 Linux 容器的 Docker Desktop。
- Docker Engine 24+、Docker Compose v2。
- Remote 主机建议 4 核 CPU、8 GB 内存、20 GB 可用磁盘。
- Spark 验证主机建议 8 GB 以上内存。
- 所有主机时间同步，主机名或固定 IP 可互相访问。

使用 `sha256sum -c SHA256SUMS`（Windows 使用 `Get-FileHash`）校验包完整性。

## 4. 部署控制面与存储

```bash
unzip fgac-control-storage-1.0.0.zip -d /opt/fgac-control
cd /opt/fgac-control/fgac-control-storage-1.0.0
cp .env.example .env
vi .env
chmod +x install.sh
./install.sh
```

将 `STORAGE_TOKEN` 换成随机值，并记录下来供 Remote 使用。检查：

```bash
curl -f http://127.0.0.1:8181/q/health
curl -f http://127.0.0.1:8003/health
docker compose logs --tail=100 polaris-fgac storage
```

从 Remote 主机验证 `curl http://CONTROL_HOST:8181/q/health` 可达。Storage 的 8003 仅允许 Remote 主机访问。

## 5. 部署独立 Remote

```bash
unzip fgac-remote-node-1.0.0.zip -d /opt/fgac-remote
cd /opt/fgac-remote/fgac-remote-node-1.0.0
cp .env.example .env
vi .env
chmod +x install.sh verify.sh
./install.sh
./verify.sh
```

`.env` 示例：

```dotenv
POLARIS_URL=http://10.10.0.10:8181
STORAGE_URL=http://10.10.0.10:8003
STORAGE_TOKEN=<与控制面相同的值>
REMOTE_BIND_IP=0.0.0.0
REMOTE_PORT=8002
```

检查 Remote 到两个上游的连通性，并从引擎主机执行 `curl http://REMOTE_HOST:8002/health`。

## 6. 部署 PostgreSQL 引擎适配器

加载镜像：

```bash
unzip fgac-engine-adapters-1.0.0.zip -d /opt/fgac-engines
cd /opt/fgac-engines/fgac-engine-adapters-1.0.0
docker load -i images/postgres-16-alpine.tar
```

把 `artifacts/fgac_pg.so` 挂载为 `/plugins/fgac_pg.so:ro`，把 `postgres/init` 挂载到 `/docker-entrypoint-initdb.d:ro`。插件必须与 PostgreSQL 16、Linux x86-64 ABI 匹配。

连接数据库后执行：

```sql
LOAD '/plugins/fgac_pg.so';
SET fgac.catalog_host = 'CONTROL_HOST';
SET fgac.catalog_port = 8181;
SET fgac.remote_host = 'REMOTE_HOST';
SET fgac.remote_port = 8002;
SET fgac.user_token = 'alice-token';

EXPLAIN (ANALYZE, VERBOSE, COSTS OFF)
SELECT d.department, SUM(o.amount)
FROM orders o JOIN departments d ON o.owner=d.owner
WHERE o.amount >= 1000 AND d.enabled
GROUP BY d.department;
```

计划中必须出现 `Custom Scan (FgacRemoteScan)`，受控表不能出现普通 `Seq Scan`。

## 7. 部署 Spark 引擎适配器

```bash
docker load -i images/spark-iceberg-latest.tar
```

将 `artifacts/fgac-spark-extension.jar` 挂载到 Spark Driver 和 Executor 可访问的位置，启动参数加入：

```text
--jars /plugins/fgac-spark-extension.jar
--conf spark.sql.extensions=demo.fgac.GovernedSparkExtension
```

同时设置：

```bash
export POLARIS_URL=http://CONTROL_HOST:8181
export CATALOG_URL=$POLARIS_URL   # 兼容当前预编译插件
export REMOTE_URL=http://REMOTE_HOST:8002
export USER_TOKEN=alice-token
```

运行 `clients/spark/demo.py`。`EXPLAIN EXTENDED` 中必须出现 `GovernedRemoteSubplan` 和物理节点 `GovernedRemote`。

## 8. 联合验收

按顺序检查：

1. Control/Storage 健康。
2. Remote 能访问 8181 和 8003，Remote 健康。
3. Alice 安全查询只返回 CN 行；Bob 只返回 US 行。
4. 伪造 schema 版本返回 409，越权列和非法算子返回 403。
5. PG 计划包含 `FgacRemoteScan`；Spark 计划包含 `GovernedRemoteSubplan`。
6. 关闭 Remote 后，受控查询必须失败关闭（fail closed），不得回退成本地直接扫描。

## 9. 运维命令

```bash
docker compose ps
docker compose logs -f --tail=200
docker compose restart
docker compose down
```

升级时先备份 `.env` 和数据目录，执行 `docker load` 加载新镜像，再运行 `docker compose up -d --force-recreate`。不要把 `.env`、存储令牌或用户令牌提交到代码仓库。

## 10. 当前限制

- Catalog 包是最小 Polaris FGAC 兼容部署，不是完整 Polaris Quarkus Server。
- 策略与用户令牌目前固定为 Alice/CN、Bob/US。
- Remote 使用 Spark local 模式执行，未包含高可用、资源隔离和作业队列。
- HTTP 返回结果采用内联行，仅适合小规模功能验证。
- 尚未实现 TLS、远程证明、CVM/TEE、集中审计和动态密钥管理。
