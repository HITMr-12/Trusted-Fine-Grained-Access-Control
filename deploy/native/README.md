# Native Linux two-node deployment

This is the deployment entry point for physical Linux servers. Docker is not
required and must not be used for benchmark runs.

## Topology

```text
Engine node E                         Remote/data node R
-----------------------------         -----------------------------
Stock Spark distribution              Minimal Polaris FGAC
Stock PostgreSQL 16                   Storage service
Spark/PG hot-loaded adapters          Remote FGAC executor
Adapter Core                          Governed Parquet snapshot
Benchmark driver                      Public benchmark data
```

The current services are suitable for functional and small-data validation.
`remote/app.py` downloads a complete Parquet object to a temporary file and
returns rows as inline JSON. Replace that path with direct object-store reads and
a streaming result protocol before using large benchmark datasets.

## Prerequisites

Both nodes need a synchronized clock and fixed host names or addresses. Node R
requires Linux, Python 3.11 or newer, a JDK 21 runtime and compiler, and enough
local storage for the immutable dataset. Node E requires Linux, Python 3.11 or
newer, JDK 17 or newer, Maven 3.9, a stock Spark 3.5.6 distribution, PostgreSQL
16, `pg_config`, a C compiler, and PostgreSQL server development headers.

The PostgreSQL plugin is ABI-specific. Build it on node E against the exact
PostgreSQL installation used by the test. Do not copy the existing Windows/demo
artifact to the physical server.

## Install node R

Clone the same release commit on R, then run as root:

```bash
cd Trusted-Fine-Grained-Access-Control
sudo deploy/native/server-r/install.sh
sudo install -m 0600 deploy/native/server-r/fgac-storage.env.example \
  /etc/fgac/storage.env
sudo install -m 0600 deploy/native/server-r/fgac-remote.env.example \
  /etc/fgac/remote.env
sudo editor /etc/fgac/storage.env /etc/fgac/remote.env
sudo systemctl enable --now fgac-polaris fgac-storage fgac-remote
```

Place the immutable Parquet snapshot below `/srv/fgac/data`. The service account
must be able to read it, but ordinary users must not be able to modify it.

## Install node E

Clone the identical commit on E, then run as root:

```bash
cd Trusted-Fine-Grained-Access-Control
sudo deploy/native/server-e/install.sh
sudo deploy/native/server-e/build-plugins.sh
sudo install -m 0600 deploy/native/server-e/fgac-adapter.env.example \
  /etc/fgac/adapter.env
sudo editor /etc/fgac/adapter.env
sudo systemctl enable --now fgac-adapter-core
```

Set the Spark environment from
`deploy/native/server-e/fgac-engine.env.example`. Start Spark with the generated
JAR and extension:

```bash
source /etc/fgac/engine.env
"$SPARK_HOME/bin/spark-submit" \
  --master 'local[8]' \
  --jars /opt/fgac/plugins/fgac-spark-extension.jar \
  --conf spark.sql.extensions=demo.fgac.GovernedSparkExtension \
  clients/spark/demo.py
```

For PostgreSQL, load the installed library in each benchmark session and set the
physical endpoints before planning governed SQL:

```sql
LOAD 'fgac_pg';
SET fgac.catalog_host = '<SERVER_R_ADDRESS>';
SET fgac.catalog_port = 8181;
SET fgac.remote_host = '127.0.0.1';
SET fgac.remote_port = 8004;
SET fgac.user_token = 'alice-token';
```

The demo tokens above are fixtures. Replace them for any non-isolated deployment.

## Startup and firewall order

Start Polaris, then Storage, then Remote, then Adapter Core, and finally the
engines. Permit E to reach R on TCP 8181 and 8002. Port 8003 is the governed
storage endpoint and must be reachable from R itself; expose it to E only during
the native baseline phase with a baseline-only credential. Revoke that credential
before the Remote FGAC phase.

Health checks:

```bash
curl --fail http://SERVER_R:8181/q/health
curl --fail http://SERVER_R:8002/health
curl --fail http://127.0.0.1:8004/health
```

## Benchmark phases

Phase 1 uses the stock engine without either plugin and explicitly applies the
same row filter and mask in SQL. Phase 2 loads the plugin and delegates governed
scans through Adapter Core. Both phases use the same files and expected result
hash. Follow `deploy/two-node/README.md` for the complete comparability contract.

Capture the output of `systemctl status`, `journalctl`, `uname -a`, Java/Python/
Spark/PostgreSQL versions, CPU topology, memory size, service environment hashes,
Git commit, data checksums, and query result hashes with every run.

## Cleanup

After exporting the experiment manifest and results, follow
`deploy/native/CLEANUP.md`. The cleanup scripts default to a dry run and require
`--yes` before they stop services or remove files.
