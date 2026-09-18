# -*- coding: utf-8 -*-
"""Replicate the baseline NATIVE path exactly (same catalog SQL + xxhash digest)
as a standalone probe to close the residual E-gap. Run twice (cold/hot)."""
import time, json, sys
from pyspark.sql import SparkSession, functions as F

label = sys.argv[1]
s = SparkSession.builder.appName('native-probe-' + label).getOrCreate()
t0 = time.perf_counter()
df = s.sql("SELECT ss_item_sk, ss_store_sk, ss_quantity, ss_sales_price, ss_net_paid "
           "FROM fgac.tpcds.store_sales WHERE ss_sales_price >= 173.50")
ready = time.perf_counter()
cols = [F.col(c) for c in df.columns]
final = (df.select(F.xxhash64(*cols).cast('decimal(38,0)').alias('h1'),
                   F.xxhash64(F.lit(9173), *cols).cast('decimal(38,0)').alias('h2'))
         .agg(F.count('*').alias('rows'), F.sum('h1').alias('sum1'), F.sum('h2').alias('sum2')))
v = {k: str(x) for k, x in final.first().asDict().items()}
end = time.perf_counter()
print('NATIVE_PROBE ' + json.dumps({'label': label, 'rows': v['rows'],
                                    'total_ms': round((end - t0) * 1000, 1),
                                    'action_ms': round((end - ready) * 1000, 1)}))
s.stop()
