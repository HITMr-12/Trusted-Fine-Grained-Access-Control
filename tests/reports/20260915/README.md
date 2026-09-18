# 2026-09-15 FGAC 部署、优化与性能报告

[全部报告目录](../README.md) · [后续 TPC-DS / MASK 独立复测](../20260918/mask_e2e_review/README.md)。本目录为 Taxi 实验，不与后续 TPC-DS 耗时混算。

请优先阅读 [三轮全场景复测与最终分析](full_alignment_20260915/FULL_RESTART_REPORT.md)：八场景、四方法独立进程，轮间各等待完整五分钟。高返回率收益跨重启成立，小结果集仍有运行阶段波动，不能用总体平均宣称全部场景稳定低于5%。此前单轮和专项测试作为阶段证据保留。

关于0.1%/1%前后结果反转，请同时阅读 [四方法隔离复测](alignment_isolated_20260915/ISOLATED_ALIGNMENT_REPORT.md)：三次独立重启未复现此前固定幅度的劣化；该复测只覆盖两个小结果集，不替代八场景报告。

## 阶段索引

| 阶段 | 报告 | 作用 |
|---|---|---|
| 1 | [部署验证](baseline_deploy_20260915/DEPLOYMENT_REPORT.md) | R/E角色交换，Polaris持久化、存储与Ranger授权验证 |
| 2 | [首次真实基线](native_baseline_20260915/PERFORMANCE_REPORT.md) | 原生授权与inline参照，修正预热口径 |
| 3 | [高返回率复核](high_return_recheck_20260915/RECHECK_REPORT.md) / [流水线分析](high_return_recheck_20260915/PIPELINE_ANALYSIS.md) | 独立复核90%和授权集合100%，定位交付链路 |
| 4 | [压缩实验](compression_20260915/COMPRESSION_REPORT.md) | 传输量显著下降，端到端无稳定收益 |
| 5 | [背景负载核查](load_audit_20260915/LOAD_AUDIT.md) | 区分当前观察与历史证据，确认背景活动差异 |
| 6 | [批次与压缩交叉测试](batch_compression_20260915/BATCH_COMPRESSION_REPORT.md) | 大批次降低局部编解码成本，未改善端到端性能 |
| 7 | [分区内逐批交付](partition_stream_20260915/PARTITION_STREAM_REPORT.md) | 消除完整分区收集屏障，同轮对照验证改善 |
| 8 | [最终统一基线](final_baseline_20260915/FINAL_PERFORMANCE_REPORT.md) | 最新FGAC与真实Ranger基线、inline同轮复测 |
| 9 | [小结果集隔离复测](alignment_isolated_20260915/ISOLATED_ALIGNMENT_REPORT.md) | COLLECT/STREAM/Native/Inline独立进程、三次重启，检验结论反转 |
| 10 | [三轮全场景复测](full_alignment_20260915/FULL_RESTART_REPORT.md) | 恢复八场景、方法隔离、两段五分钟间隔，分析最终结论与剩余波动 |

各阶段报告保留当时的结果和限制；早期的百分比不是当前最终结论。不能把不同阶段的FGAC与原生耗时组合成新的劣化率。

## 代码与运行范围

- `streaming/server.py`：标准Arrow IPC可选LZ4/Zstd压缩，默认不压缩。
- `streaming/spark_batches.py`、`streaming/jvm/PartitionArrowStream.java`：可选分区内逐批交付；当前仅验证Spark 3.5.8 local模式，外部Flight协议保持一致。
- `streaming/test_partition_integration.py`：生成数据的增量交付、取消、错误传播等检查。
- 构建与启用方式见仓库 [streaming/README.md](../../../streaming/README.md)。helper jar必须追加到已有Spark依赖，不能覆盖Iceberg等依赖。

最新方案的额外R计算资源是用户明确接受的部署优势；未设置总CPU配额或绑核。原生产服务保持运行，隔离测试服务在完成后清理。

## 证据和复现

Git保存阶段报告、汇总、查询记录、协议和脚本。大体积Spark任务事件、全进程资源采样及二进制包保留在测试环境；各阶段`artifact-sha256.json`对应原始工作区/服务器产物，不应与Git规范化后的文本字节混为一谈。

R运行目录：`/data1/lyb/fgac-lab/runs/`；E运行目录：`/home/lyb/fgac-lab/runs/`。相关子目录为`native-baseline-0915`、`high-return-recheck-0915`、`compression-0915`、`batch-compression-0915`、`partition-stream-0915`及`final-baseline-0915`。取回对应`evidence-R.tar.gz`/`evidence-E.tar.gz`并核对哈希，可补齐分析脚本依赖的任务与资源证据。

脚本内的绝对路径对应本次实验环境。复现需先配置Java11、Spark3.5.8、Iceberg1.7.2、Arrow19.0.1、Polaris和Ranger；凭据只从本地受保护文件读取，不随Git发布。INLINE显式写入策略条件，是非安全等价的理想参照，并不保证每次测量都快于原生授权。
