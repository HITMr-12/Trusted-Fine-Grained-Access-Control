# -*- coding: utf-8 -*-
"""Add high-selectivity bench_full cases (60% / 75% / 90%) using data quantiles,
with exact measured row counts; writes cases_selectivity.json (all 8 cases)."""
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

r = Path('/home/lyb/fgac-lab')
d = r / 'deploy/fgac-tpcds-bench'
spark = SparkSession.builder.appName('add-high-cases').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

cases = json.loads((d / 'cases.json').read_text())
base = next(c for c in cases if c['name'] == 'bench_full_5')

df = spark.table('fgac.tpcds.store_sales')
qs = df.approxQuantile('ss_sales_price', [0.4, 0.25, 0.1], 0.005)
targets = [('bench_full_06', 0.6, qs[0]), ('bench_full_075', 0.75, qs[1]), ('bench_full_09', 0.9, qs[2])]

new_cases = []
for name, target, q in targets:
    thr = round(float(q), 2)
    rows = df.filter(F.col('ss_sales_price') >= thr).count()
    c = json.loads(json.dumps(base))
    c['name'] = name
    c['target'] = target
    c['threshold'] = thr
    c['rows'] = rows
    c['actual_ratio'] = round(rows / c['authorized_rows'], 6)
    c['business_sql'] = 'ss_sales_price >= %.2f' % thr
    c['plan']['root']['input']['condition']['right']['value'] = thr
    new_cases.append(c)
    print('NEWCASE %s' % json.dumps({'name': name, 'threshold': thr, 'rows': rows,
                                     'ratio': c['actual_ratio']}), flush=True)

out = [c for c in cases if c['principal'] == 'bench_full'] + new_cases
out.sort(key=lambda c: c['rows'])
(d / 'cases_selectivity.json').write_text(json.dumps(out, indent=1, ensure_ascii=False))
print('WROTE %d cases' % len(out), flush=True)
spark.stop()
