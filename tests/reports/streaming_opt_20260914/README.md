# 流式提前预取：实现与复测归档

测试日期：2026-09-14；归档分支：`prf-test-0915`。

- [性能报告](OPTIMIZATION_REPORT.md)：优化原理、统一口径、结果和限制。
- [场景计划](round1/cases.json)：七个场景的实际条件和返回行数。
- [逐次测量](round1/records.json)、[结果校验](round1/validation.json)、[配对数据](round1/pairs.csv)。
- [服务端审计](r/audit.jsonl)、[阶段记录](r/stages.jsonl)、[探针汇总](round1/probe_summary.json)。
- [测试结果](unit_tests.txt)：隔离环境中 15 项单元测试通过。

从仓库根目录使用 Python 标准库重新计算配对统计及置信区间：

```bash
python tests/reports/streaming_opt_20260914/analyze.py
```

该命令只分析归档数据，不连接远端，也不重新执行性能测试。

`benchmark.py`、`flight_bridge.py` 和部署版 adapter/compiler 是当时的测试脚本快照。
实际运行依赖原隔离环境的 Spark/Iceberg 配置、数据快照、身份配置、
`phase1_baseline_benchmark` 和 `fgac.bench.ArrowSocketSource` 测试 JAR；
这些脚本不是可直接部署到任意环境的一键启动包。服务源码位于仓库 `streaming/`。

本归档未收录大型 Spark 事件日志、原始进程采样及 SSH 运维文件；完整原始采集物
仍保留在本地工作区 `output/streaming_opt_20260914/`。本次发布没有替换现有部署。
