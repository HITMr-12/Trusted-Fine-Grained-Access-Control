# Two-node deployment and benchmark plan

This milestone deliberately evaluates the software Remote FGAC architecture and
does not depend on confidential-computing hardware.

## Fixed node roles

```text
Engine node (E)                       Remote/data node (R)
-------------------------------       -----------------------------
Unmodified Spark/PostgreSQL           Polaris FGAC
Hot-loaded engine adapter             Storage service
Adapter Core                          Immutable Parquet snapshot
Benchmark driver                      Remote FGAC executor
```

Use the same physical machines for every run. Do not swap roles between the
baseline and FGAC phases.

## Phase 1: native baseline

The unmodified engine on E reads the immutable Parquet snapshot from R. It does
not load either adapter and does not call Adapter Core or Remote. Benchmark SQL
must explicitly contain a filter and mask equivalent to the policy used in phase
2. This is a performance reference, not a secure deployment.

Issue a baseline-only storage credential before this phase and revoke it before
phase 2.

## Phase 2: Remote FGAC

The engine loads the Spark or PostgreSQL adapter. Governed scans are converted to
Remote Plan, sent through Adapter Core to Remote on R, authorized again against
Polaris, and executed over the same Parquet snapshot. E must not have a governed
storage credential in this phase. A Remote failure must fail closed.

## Comparability contract

Both phases must pin:

- engine and dependency versions;
- data snapshot and Parquet layout;
- query parameters and expected result hash;
- total query CPU and memory budget across E and R;
- concurrency, warm-up count, measured repetitions, and cache condition;
- NIC, MTU, storage device, compression, and partitioning.

Record CPU time on both machines. Remote FGAC moves work from E to R, so E-only
CPU utilization is not a fair cost comparison.

Required measurements include end-to-end latency, first-batch latency, p50/p95/
p99, throughput, bytes read from storage, bytes returned to E, serialized bytes,
result rows, result hash, CPU time per node, peak memory, spill bytes, and Remote
request count.

At minimum, test scan-heavy, high-selectivity, low-selectivity, projection, and
governed/public join queries at concurrency 1, 4, 8, and 16. The current Remote
Plan implementation supports governed scan, filter, and project; aggregate or
join must not be reported as remotely executed until the protocol and compiler
actually support them.

## Existing deployment material

The current component-level Compose files and offline-install instructions remain
under `deploy/cluster`. Before a physical-server run, copy the example environment
files, replace all demo values, and restrict Storage access so only R can read
governed objects during phase 2.
