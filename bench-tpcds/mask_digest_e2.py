# -*- coding: utf-8 -*-
"""Single-pass variant of mask_digest_e: one agg job computes count + all
column aggregates (no separate df.count() pass). Same ref format.
Usage: spark-submit mask_digest_e2.py <dir> <filter_expr> <ref_json> <label>
"""
import json, sys, time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

d, expr, ref_path, label = sys.argv[1:5]
light = len(sys.argv) > 5 and sys.argv[5] == 'light'
ref = json.load(open(ref_path))['ref_agg']
DEC = {'ss_sales_price', 'ss_net_paid'}
INTS = ['ss_item_sk', 'ss_store_sk', 'ss_quantity', 'ss_sales_price', 'ss_net_paid']

spark = SparkSession.builder.appName('mask-digest2-' + label).getOrCreate()
t0 = time.time()
df = spark.read.parquet(d).filter(expr)
exprs = [F.count(F.lit(1)).alias('n')]
for c in INTS:
    if light and c not in ('ss_quantity', 'ss_sales_price'):
        continue
    col = F.col(c) * 100 if c in DEC else F.col(c)
    exprs += [F.sum(col).cast('decimal(38,0)').alias('s_' + c),
              F.min(col).cast('decimal(38,0)').alias('n_' + c),
              F.max(col).cast('decimal(38,0)').alias('x_' + c)]
row = df.agg(*exprs).collect()[0]
ms = (time.time() - t0) * 1000
ok = int(row['n']) == ref['rows']
if not light:
    for c in INTS:
        g = {'sum': int(row['s_' + c]), 'min': int(row['n_' + c]), 'max': int(row['x_' + c])}
        r = ref['cols'][c]
        if not (g['sum'] == int(r['sum']) and g['min'] == r['min'] and g['max'] == r['max']):
            ok = False
            print('MISMATCH', c, 'ref', r, 'got', g)
print('E_DIGEST2 ' + json.dumps({'label': label, 'equal': ok, 'rows': int(row['n']), 'ms': round(ms, 1)}))
spark.stop()
