# 2026-09-18 MASK 独立审查与干净环境端到端复测

审查分支 `mask-e2e-0918`，被测代码固定为 `839cb49385ec2c218c8efade3983376f4d0bad66`。本目录是对 09-17 历史 MASK 报告的独立复核；没有修改被测授权或接收完成协议。

## 阅读顺序

1. [与历史报告逐项对照](COMPARISON.md)：哪些结论保留、哪些需要缩小边界、哪些被新证据否定。
2. [完整复测报告](REVIEW.md)：清理重建、数据复用依据、端到端协议、性能表、故障结果及修正建议。
3. [结构化汇总](results-summary.json)与下面的原始证据：独立核验报告数字。

## 结论边界

144 次正常端到端查询摘要一致，支持固定布局的数据通路可行性。MASK 缺少等价授权、断流可误提交部分结果，原对齐脚本的 NATIVE 又未启用 Ranger，因此不能认可完整系统的安全等价比较或全场景可靠性。资源对齐组的中高选择率收益不应替代上述判断。

## 证据目录

| 目录或文件 | 用途 | 范围 |
|---|---|---|
| [resource_aligned](evidence/resource_aligned) | FGAC / MASK2 相同 CPU 配额；NATIVE 为直接读取参考 | 8 场景，24 次预热、72 次正式 |
| [original_config](evidence/original_config) | NATIVE / FGAC / MASK16 原 CPU 部署方式；另含 MASK1 补测 | 3 场景，12 次预热、36 次正式；MASK1 不与 MASK16 同轮交错 |
| [faults](evidence/faults) | 真实端到端授权拒绝、断流与配置修正对照 | 故障结果不进入正常性能统计 |
| [environment](evidence/environment) | 数据一致性、重建补丁、清理及制品哈希 | 存储核验与环境证据，不作为性能测试 |
| [evidence-manifest.json](evidence-manifest.json) | 发布证据的文件哈希 | 按 Git 中 LF 字节校验 |

在仓库根目录运行 `python tests/reports/20260918/mask_e2e_review/verify_results.py`，只进行离线证据核验，不连接远端，也不运行单元测试或新的性能测试。复测为新进程预热后的暖态，非清空整机页缓存的冷启动。

历史实验路径保持不变，避免破坏原始脚本与引用。参见[统一报告索引](../../README.md)和[09-18 索引](../README.md)。
