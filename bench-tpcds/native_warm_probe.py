# -*- coding: utf-8 -*-
"""Warmup probe: run the exact baseline NATIVE query 6 times in one persistent
session, report per-rep action time. Settles whether the baseline's 3.95s
median is a warm-session artifact."""
import time, json
from pyspark.sql import SparkSession, functions as F

s = SparkSession.builder.appName('native-warm-probe').getOrCreate()
reps = []
for i in range(6):
    t0 = time.perf_counter()
    df = s.sql("SELECT ss_item_sk, ss_store_sk, ss_quantity, ss_sales_price, ss_net_paid "
               "FROM fgac.tpcds.store_sales WHERE ss_sales_price >= 173.50")
    cols = [F.col(c) for c in df.columns]
    final = (df.select(F.xxhash64(*cols).cast('decimal(38,0)').alias('h1'),
                       F.xxhash64(F.lit(9173), *cols).cast('decimal(38,0)').alias('h2'))
             .agg(F.count('*').alias('rows'), F.sum('h1').alias('sum1'), F.sum('h2').alias('sum2')))
    v = final.first()
    reps.append(round((time.perf_counter() - t0) * 1000, 1))
print('WARM_PROBE ' + json.dumps({'reps_ms': reps, 'rows': str(v['rows'])}))
s.stop()
