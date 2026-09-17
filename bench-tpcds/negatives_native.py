# -*- coding: utf-8 -*-
"""NATIVE 模式负向检查 v2（列必须被最终计划引用，授权检查才会触发）：
  1) denied 主体执行业务语句(5列SELECT+WHERE, collect)  -> 期望 AccessControlException
  2) alice 查询未授权列 ss_ticket_number (LIMIT, collect) -> 期望 AccessControlException
注意：count() 会把列裁剪掉，Kyuubi authz 对无列引用的聚合不建权限对象 -> 不检查（已实证）。
"""
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tpcds-negative-native-v2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
principal = os.environ.get("HADOOP_USER_NAME", "unknown")
checks = []


def expect_denied(label, sql):
    try:
        rows = spark.sql(sql).collect()
        checks.append((label, False, f"未被拒绝，返回 {len(rows)} 行"))
    except Exception as e:
        msg = str(e)
        ok = "AccessControlException" in msg or "denied" in msg.lower() or "not authorized" in msg.lower()
        checks.append((label, ok, "AccessControlException" if ok else type(e).__name__ + ": " + msg[:150]))


if principal == "denied":
    expect_denied("denied 执行业务语句",
                  "SELECT ss_item_sk, ss_store_sk, ss_quantity, ss_sales_price, ss_net_paid "
                  "FROM fgac.tpcds.store_sales WHERE ss_sales_price >= 173.50")
elif principal == "alice":
    expect_denied("alice 查未授权列 ss_ticket_number",
                  "SELECT ss_ticket_number FROM fgac.tpcds.store_sales WHERE ss_store_sk = 1 LIMIT 3")
else:
    raise SystemExit("用法: HADOOP_USER_NAME=denied|alice 提交本脚本")

spark.stop()
for label, ok, detail in checks:
    print(("NEGATIVE_PASS " if ok else "NEGATIVE_FAIL ") + label + " | " + detail)
