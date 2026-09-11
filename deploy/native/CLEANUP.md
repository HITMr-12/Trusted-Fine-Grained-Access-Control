# Native deployment cleanup

The cleanup procedure removes the FGAC product from both physical Linux nodes
without relying on Docker. Run it only after benchmark results and configuration
manifests have been exported to an approved location.

## What is removed by default

On remote/data node R:

- `fgac-polaris`, `fgac-storage`, and `fgac-remote` systemd services;
- their unit files under `/etc/systemd/system`;
- `/etc/fgac/storage.env` and `/etc/fgac/remote.env`;
- `/opt/fgac` and `/var/lib/fgac`;
- service-specific private temporary directories;
- `/var/log/fgac`, if it exists.

On engine node E:

- the `fgac-adapter-core` systemd service and unit file;
- `/etc/fgac/adapter.env` and `/etc/fgac/engine.env`;
- `/opt/fgac`, including the Spark adapter JAR;
- `fgac_pg.so` from the directory reported by `pg_config --pkglibdir`;
- locally generated Spark/PG plugin build outputs in this checkout;
- service-specific private temporary directories;
- `/var/log/fgac`, if it exists.

The scripts do not uninstall system Python, Java, Maven, Spark, PostgreSQL,
compilers, or operating-system packages because those installations are not
owned by this project.

## Preserved unless explicitly requested

- `/srv/fgac/data`: use `--purge-data` on R to remove the dataset;
- `/srv/fgac/results`: use `--purge-results` on E to remove collected results;
- service users and groups: use `--purge-users`;
- the Git checkout and shared Maven/pip caches.

Use the preservation defaults when the files belong to a shared laboratory data
or results repository. For a disposable test machine, use all purge flags.

## Before cleanup

1. Stop benchmark submission and close every PostgreSQL session that loaded
   `fgac_pg`. The E cleanup aborts if the library is still mapped by a process.
2. Export result CSV/JSON, logs needed for analysis, result hashes, Git commit,
   data checksums, service environment hashes, and host facts.
3. Revoke the baseline storage credential and every test user token in the
   external authorization system. Deleting local files does not revoke a token.
4. Verify that no other application uses `/opt/fgac`, `/etc/fgac`, the `fgac`
   accounts, or TCP ports 8002-8004 and 8181.

## Execute

First preview on each node:

```bash
sudo deploy/native/cleanup/cleanup-server-e.sh
sudo deploy/native/cleanup/cleanup-server-r.sh
```

Then execute the standard product cleanup:

```bash
# Engine node E
sudo deploy/native/cleanup/cleanup-server-e.sh --yes

# Remote/data node R
sudo deploy/native/cleanup/cleanup-server-r.sh --yes
```

For a complete purge on disposable benchmark machines:

```bash
# Engine node E: also delete collected results and the service account
sudo deploy/native/cleanup/cleanup-server-e.sh \
  --purge-results --purge-users --yes

# Remote/data node R: also delete the Parquet snapshot and service account
sudo deploy/native/cleanup/cleanup-server-r.sh \
  --purge-data --purge-users --yes
```

Run the appropriate verification script afterwards. It fails if a service,
process, listening port, product directory, environment file, or adapter remains.

```bash
sudo deploy/native/cleanup/verify-server-e-clean.sh
sudo deploy/native/cleanup/verify-server-r-clean.sh --expect-data-removed
```

## PostgreSQL process state

The plugin is loaded into individual PostgreSQL backend processes. Unlinking the
`.so` file does not remove it from an already-running process. Close all benchmark
connections first. If the deployment configured session preloading outside this
repository, remove that configuration and restart the dedicated benchmark
PostgreSQL instance before running the cleanup again. The cleanup scripts do not
restart or uninstall a shared database service.

## Journald and secure erasure limitations

systemd's default journal may retain historical stdout/stderr records after a unit
is removed. Journald does not provide safe per-unit deletion from its shared binary
journal. Do not vacuum the global journal on a shared server. On a disposable
benchmark host, an administrator may rotate and vacuum the complete journal under
the site's audit-retention policy.

Ordinary file deletion also does not guarantee physical erasure on SSD, RAID,
copy-on-write, snapshot, or remote storage. If the benchmark contains sensitive
data or credentials, place `/srv/fgac`, `/var/lib/fgac`, `/var/log/fgac`, and
temporary result storage on an encrypted disposable volume before testing; final
cleanup then includes destroying that volume's key and deleting storage snapshots.

The current minimal Polaris service stores policies in memory and has no catalog
database to purge. A future persistent Polaris deployment must add its database,
backup, and transaction-log locations to this inventory.

After verification, remove the Git checkout manually if the physical machine was
created solely for this experiment. The cleanup script deliberately does not
recursively delete the directory from which it is running.
