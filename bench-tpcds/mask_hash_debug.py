# -*- coding: utf-8 -*-
"""Full-scan (bench_full_1_0) parity debug: NULL counts in native across all 5
cols, sentinel presence in delivery, per-column xxhash compare after restoring
-2 -> NULL on whichever columns need it."""
from pyspark.sql import SparkSession, functions as F

COLS = ['ss_item_sk', 'ss_store_sk', 'ss_quantity', 'ss_sales_price', 'ss_net_paid']

s = SparkSession.builder.appName('mask-hash-debug3').getOrCreate()
s.sparkContext.setLogLevel('ERROR')

native = s.sql('SELECT %s FROM fgac.tpcds.store_sales' % ', '.join(COLS))
nulls = native.agg(*[F.sum(F.col(c).isNull().cast('int')).alias(c) for c in COLS]).first().asDict()
print('NATIVE full-scan nulls:', nulls)

mask_raw = s.read.parquet('/home/lyb/fgac-lab/runs/mask-e2e/delivery_s100')
mins = mask_raw.agg(*[F.min(c).alias(c) for c in COLS]).first().asDict()
print('MASK delivery mins:', {k: str(v) for k, v in mins.items()})

mask = mask_raw
for c in COLS:
    mask = mask.withColumn(c, F.when(F.col(c) != -2, F.col(c)))

for tag, df in [('NATIVE', native), ('MASK', mask)]:
    aggs = [F.sum(F.xxhash64(F.col(c)).cast('decimal(38,0)')).alias(c) for c in df.columns]
    row = df.agg(*aggs).first().asDict()
    print(tag, 'hashes:', {k: str(v) for k, v in row.items()})
print('ANTIJOIN n_minus_m=%d m_minus_n=%d'
      % (native.exceptAll(mask).count(), mask.exceptAll(native).count()))
s.stop()
