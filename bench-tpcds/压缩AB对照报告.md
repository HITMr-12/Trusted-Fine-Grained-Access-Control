# 压缩 A/B 对照报告（FGAC 明文 Arrow vs LZ4 vs ZSTD）

- 运行：`runs/tpcds-ab-round1`（E 节点 `/home/lyb/fgac-lab/runs/tpcds-ab-round1`）
- 时间：2026-09-16，服务器 UTC
- 结论一句话：**传输压缩基本收不回大 case 的 FGAC 性能税**——LZ4 在最大 case（bench_full_1_0）仅回收约 13%，ZSTD 净收益约为零；税的本质是端到端 Arrow 序列化/反序列化的 CPU 开销，压缩只是把瓶颈从 R 端转移到了 E 端，而不是消除了它。

## 1. 实验设计

- 4 路同机交错对照：NATIVE（基线）、FGAC（明文 Arrow IPC）、FGAC-LZ4、FGAC-ZSTD（level 1），全部 local[2]，跳过 INLINE。
- 15 个 case（alice/bob/bench_full × 001/01/1/1_0/5），SQL 与权限策略与第一轮压测完全相同（见 [cases](ab-evidence/cases.json)）。
- Remote 侧三端口 codec 变体：NONE=18833 / LZ4=18834 / ZSTD=18835，单 Spark 进程 + 共享 scan_slot；压缩实现为 Arrow IPC `IpcWriteOptions`（codec 帧级压缩），客户端透明解压。
- 负载：warmup 20 轮 + 正式 6 rep，每 case 四路交错执行，摘要（行数 + 双和校验）四路全等断言。
- 完整性：done.json `all_paired_results_equal=true`，360 条正式记录；快照前后一致、Ranger 策略前后一致（[snapshot-before](ab-evidence/snapshot-before.json) / [snapshot-after](ab-evidence/snapshot-after.json) / [ranger-policies-before](ab-evidence/ranger-policies-before.json) / [ranger-policies-after](ab-evidence/ranger-policies-after.json)）。

## 2. 主结果（中位数，ms）

（表见 [ab-table](ab-table.md)，同 [summary.csv](ab-evidence/summary.csv)）

要点：

- 小 case（≤28MB Arrow 流，alice/bob 全系 + bench_full_001/01）：压缩与明文差异在 ±2% 噪声带内，无实际影响。
- bench_full_1（144MB）：明文已与 NATIVE 持平（1.011），压缩后 1.02，无恶化也无收益。
- bench_full_5（720MB）：明文税 1.250，LZ4 1.241（仅回收 3%），ZSTD 1.261（倒挂 -4%）。
- bench_full_1_0（1,442MB）：明文税 1.435，LZ4 1.377（回收 13%，14,048→13,478ms），ZSTD 1.438（回收约 0%）。

## 3. 关键发现：税去哪了（bench_full_1_0 桥接分解，ms）

| 模式 | total | bridge_next_wait（等远端） | bridge_ipc_write（E端写出） |
|---|---|---|---|
| FGAC 明文 | 14,048 | 8,592 | 4,383 |
| FGAC-LZ4 | 13,478 | **5,981（-2,611）** | **6,992（+2,609）** |
| FGAC-ZSTD | 14,080 | **5,809（-2,783）** | **7,530（+3,147）** |

压缩确实把"等远端"的时间砍掉了约 2.6–2.8 s（与"按字节线性的传输税 ≈3.8ms/MB、LZ4 线率约 50%"的推算吻合），但 E 端解压+写出的 CPU 开销几乎等额回填。**管道总 CPU 是守恒的，压缩只改变了它在 R/E 两端的分布。**

推论：当前链路上，大 case 的税不是链路带宽税，而是 Arrow IPC 序列化→传输→解压→写出的字节搬运税。要真正收税，方向是**减少需要搬运的字节**（远端谓词/聚合下推、列裁剪已做到位后的进一步算子下推），或提升 E 端消费速度（NVMe/更快反序列化），而不是在传输层压缩。

## 4. LZ4 vs ZSTD 取舍

两者压测层面基本等价，ZSTD(level 1) 压缩率未见转化为净时间收益，且 rep 间方差略大（bench_full_1_0 各 rep：ZSTD 13,109–15,312 vs LZ4 13,192–14,158）。**若将来启用传输压缩，选 LZ4，不选 ZSTD。**

## 5. 诚实口径

- 仅 6 rep，±5–8% 的 rep 间波动存在；LZ4/NONE 比值在 0.96–1.01 之间，除 bench_full_1_0 外多数在噪声带内。
- E 节点无 NVMe 的介质不对称仍在（第一轮已声明），E 端 ipc_write 回填幅度会随 E 端介质/CPU 变化。
- 跨机 wire 字节数无直接指标（压缩发生在 IPC 序列化层，bridge 统计的是解压后字节）；上文线率 50% 为按时间差与税率的推算，非实测。
- LZ4/ZSTD 均作用于 Arrow IPC 帧级，对 decimal 列为主的该 schema 可能并非最优压缩位点。

## 6. 证据索引

- 逐条记录：[records](ab-evidence/records.jsonl)（480 条 = 120 warmup + 360 formal）
- 汇总：[summary.csv](ab-evidence/summary.csv) / [summary.json](ab-evidence/summary.json)
- 协议与完成态：[protocol](ab-evidence/protocol.json) / [done](ab-evidence/done.json)（ok=true，all_paired_results_equal=true）
- case 定义与负向控制：[cases](ab-evidence/cases.json) / [negative-controls](ab-evidence/negative-controls.json)
- 前置报告：[第一轮压测报告](第一轮压测报告.md)、[套件说明](README.md)
