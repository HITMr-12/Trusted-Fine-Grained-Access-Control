# -*- coding: utf-8 -*-
"""Pipelined E-side consumer (directory-polled): digests arriving Parquet files
in batches with a persistent SparkSession, overlapping consumption with the
R-side production/transfer stream. Stops when <dir>/.done appears and all
files are processed.

Digest is IDENTICAL to the NATIVE baseline sink (tpcds_worker.py):
  xxhash64(*cols) -> sum1, xxhash64(9173, *cols) -> sum2, count(*) -> rows
so MASK-pipeline and NATIVE are measured under exactly the same read config
(same SPARK_CONF_DIR session, same filter, same digest). Per-batch digests are
mergeable (rows/sum1/sum2 are all additive).

Usage: spark-submit pipeline_e.py <indir> <filter_expr> <label> [batch_size] [ref_json]
  ref_json (optional): {"rows": int, "sum1": str, "sum2": str} to verify against.
"""
import json, sys, time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

indir, expr, label = sys.argv[1:4]
batch_size = int(sys.argv[4]) if len(sys.argv) > 4 else 4
ref = json.loads(sys.argv[5]) if len(sys.argv) > 5 else None
indir = Path(indir)
# Same NULL restoration as tpcds_worker MASK mode: the delivery materializes
# NULLs as the -2 sentinel; restore NULL semantics before filter/digest so the
# digest input is value-identical to NATIVE.
MASK_NULLABLE = ['ss_store_sk', 'ss_quantity', 'ss_sales_price', 'ss_net_paid']
MASK_NULLFILL = -2

t_start = time.perf_counter()
spark = SparkSession.builder.appName('mask-pipe-' + label).getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
t_session = time.perf_counter()
print('PIPE_STAGE session_ready_s=%.2f' % (t_session - t_start), flush=True)

rows = 0
sum1 = 0
sum2 = 0
processed = set()
batches = []
e_work = 0.0
first_batch_at = None
while True:
    files = sorted(p for p in indir.glob('*.parquet'))
    pending = [p for p in files if p.name not in processed]
    done_marker = (indir / '.done').exists()
    if len(pending) >= batch_size or (done_marker and pending):
        t0 = time.perf_counter()
        df = spark.read.parquet(*[str(p) for p in pending])
        for c in MASK_NULLABLE:
            df = df.withColumn(c, F.when(F.col(c) != MASK_NULLFILL, F.col(c)))
        df = df.filter(expr)
        cols = [F.col(c) for c in df.columns]
        final = (df.select(F.xxhash64(*cols).cast('decimal(38,0)').alias('h1'),
                           F.xxhash64(F.lit(9173), *cols).cast('decimal(38,0)').alias('h2'))
                 .agg(F.count('*').alias('n'), F.sum('h1').alias('s1'), F.sum('h2').alias('s2')))
        v = final.first()
        dt = (time.perf_counter() - t0) * 1000
        if first_batch_at is None:
            first_batch_at = t0 - t_start
        e_work += dt
        rows += int(v['n'])
        sum1 += int(v['s1'])
        sum2 += int(v['s2'])
        batches.append({'files': len(pending), 'digest_ms': round(dt, 1),
                        'start_s': round(t0 - t_start, 2),
                        'done_s': round(time.perf_counter() - t_start, 2)})
        print('PIPE_STAGE batch files=%d digest_ms=%.0f start_s=%.2f done_s=%.2f'
              % (len(pending), dt, t0 - t_start, time.perf_counter() - t_start),
              flush=True)
        processed.update(p.name for p in pending)
    elif done_marker:
        break
    else:
        time.sleep(0.1)

wall = time.perf_counter() - t_start
digest = {'rows': str(rows), 'sum1': str(sum1), 'sum2': str(sum2)}
ok = None
if ref:
    ok = (digest['rows'] == str(ref['rows']) and digest['sum1'] == str(ref['sum1'])
          and digest['sum2'] == str(ref['sum2']))
print('PIPE ' + json.dumps({'label': label, 'equal': ok, 'digest': digest,
                            'wall_s': round(wall, 2),
                            'session_startup_s': round(t_session - t_start, 2),
                            'first_batch_at_s': round(first_batch_at, 2) if first_batch_at is not None else None,
                            'e_work_ms': round(e_work, 1),
                            'batches': batches}))
spark.stop()
