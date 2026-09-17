# TPC-DS store_sales 公平基准套件（bench-tpcds）

2026-09-16 建立，供 FGAC / 基线（NATIVE）/ inline 三方案横向性能测试复用。
口径完全对齐 0915 taxi 基线（等值行策略 + 业务阈值梯度 + 固定列投影 + xxhash64 双盐摘要汇）。

## 1. 运行语句（cases.json，15 案）

- **受控表**：`fgac.tpcds.store_sales`（28,800,991 行 / 23 列）；受控关系 `lake.sales.store_sales`。
- **主体 × 梯度**：alice（行策略 ss_store_sk=1，536,919 行）、bob（=2，541,263 行）、bench_full（无行策略，全表 28,800,991 行）× 选择性 {0.1%, 1%, 10%, 50%, 100%}。
- **业务谓词**：`ss_sales_price >= T`，阈值 {173.50, 143.70, 88.80, 27.60}，实测梯度误差 ±5% 内（阈值来自全表/分位数调优，`gen_cases.py` 可复现）。
- **投影列**（5 列，同时是 Ranger 列白名单与 FGAC 可释放列）：ss_item_sk, ss_store_sk, ss_quantity, ss_sales_price, ss_net_paid。
- **摘要汇**（三模式一致）：`xxhash64(*cols)` + `xxhash64(9173,*cols)` 双盐，聚合 count/sum → digest(sum1,sum2)。
- **执行形态**：NATIVE/INLINE 为同一 SQL 文本（INLINE 把行策略内联进 WHERE）；FGAC 为 plan v2（governed_scan→filter→project）。
- 每个 case 含 `rows`（实测基线）、`plan`、`business_sql`、`row_filter_sql`，是后续 36-rep 正式压测（对齐 0915 的 protocol）的输入。

## 2. 权限策略

| 层 | 位置 | 内容 |
|---|---|---|
| Ranger（NATIVE 强制） | E 节点服务 `fgac_bench_0915` 策略 24/25 | 列级 SELECT 白名单（3 主体×5 列）；行过滤 alice=ss_store_sk 1、bob=2 |
| Polaris FGAC（授权） | R 节点 sidecar :18184 | 受控关系 `lake.sales.store_sales`（storage_object governed.store_sales.v1）；token→主体→行策略同上 |
| Remote 数据面 | R 节点 :18833 | `FGAC_TABLE_LAKE_SALES_STORE_SALES=fgac.tpcds.store_sales`；背压补丁 + `spark.driver.maxResultSize=0` |

部署变更均为**附加式**：0915 taxi 旧策略、旧 Remote（:18815）未动；`configure_policies_tpcds.py` 幂等。

## 3. 可用性验证结论（2026-09-16，全部实测）

- **语句可用性**：15/15 案在 NATIVE（alice/bob/bench_full 分别提交）、INLINE、FGAC 三模式全部跑通，行数与 cases.json 基线完全一致。
- **跨模式一致性**：15/15 案三模式 digest（sum1/sum2）与 schema 完全一致（`compare_modes.py`，证据 `evidence-*.jsonl`）。
- **负向检查**（4/4 通过）：
  - NATIVE denied 执行业务语句 → AccessControlException
  - NATIVE alice 查未授权列 ss_ticket_number → AccessControlException
  - FGAC 无效 token → Unauthenticated
  - FGAC 投影未授权列 → Unauthorized
- **压测前置障碍已排**：全表扫描 28.8M 行 / 1.44GB 可稳定流完（裸 Flight 客户端 20.2s）。

## 4. 两个重要发现（写进测试计划）

1. **`spark.driver.maxResultSize` 默认 1g 硬切断 collectAsArrow 流**：在 988 MiB 处确定性中止（服务端报 S3A AbortedException，客户端表现为 gRPC metadata 超限的误导性报错）。已在 Remote conf 置 0，并给 Flight 服务端加背压补丁（`patch_backpressure.py`，FGAC_MAX_INFLIGHT_BATCHES/BYTES 可调）。
2. **Kyuubi authz 对"最终计划无列引用"的聚合不建权限对象 → 不检查**：`df.count()` 会把投影列裁剪掉，denied 也能查出 27668 行；换 collect()（列被引用）即正确拒绝。**公平性含义**：正式压测的摘要汇必须保持列引用（现有 digest 设计天然满足），且这一点本身是 NATIVE 基线与 FGAC 的行为差异（FGAC 路径无此绕过），值得在报告中单独讨论。

## 5. 复用方式（正式压测）

1. `python3 configure_policies_tpcds.py`（E 节点，幂等）
2. 确认 R 节点 :18184（sidecar）、:18833（Remote）存活；重启命令见 serve_remote_tpcds.py 头注
3. 三模式提交模板见 `validate_tpcds_bench.py` 头注；正式 36-rep 交错压测仿照 `final_baseline_20260915/benchmark.py`，cases 换用本套件，参考 digest 取 `evidence-native-*.jsonl`

## 6. 文件索引

| 文件 | 说明 |
|---|---|
| cases.json / gen_cases.py | 15 案定义 / 生成器 |
| configure_policies_tpcds.py | Ranger 策略（幂等） |
| MinimalPolarisFgac.java | sidecar 补丁版（含新受控关系） |
| deployed_adapter.py / serve_remote_tpcds.py | Remote 按关系选源 / TPC-DS Remote 启动脚本 |
| patch_backpressure.py | Flight 服务端背压补丁 |
| flight_bridge_tpcds.py / flight_bridge.py | TPC-DS 桥（参数化 schema）/ 原始 taxi 桥 |
| validate_tpcds_bench.py / compare_modes.py / negatives_*.py | 三模式执行器 / 比对 / 负向检查 |
| evidence-*.jsonl / evidence-ranger-policies.json | 验证证据 |
