# -*- coding: utf-8 -*-
"""Resident pipelined E-side consumer: one persistent SparkSession serves an
unbounded sequence of MASK queries. Watches <root>/run_<label>/ directories;
for each, consumes arriving files in small batches (default 1 = immediate),
computing the SAME xxhash64 digest as tpcds_worker NATIVE/MASK/FGAC. On .done
+ drain, prints one PIPE line (JSON) and moves to the next run dir.

Resident == same deployment posture as NATIVE catalog session and FGAC Flight
service; session startup is paid once, not per query.

Usage: spark-submit pipeline_e_resident.py <root>
Stdout PIPE lines: {"label","digest","wall_s","first_batch_lag_s",...}
"""
import json, sys, time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

root = Path(sys.argv[1])
EXPR = {'s001': 'ss_sales_price >= 173.50', 's01': 'ss_sales_price >= 143.70',
        's10': 'ss_sales_price >= 88.80', 's477': 'ss_sales_price >= 27.60',
        's575': 'ss_sales_price >= 19.46', 's718': 'ss_sales_price >= 9.97',
        's86': 'ss_sales_price >= 3.12', 's100': 'true',
        'warm': 'ss_sales_price >= 143.70'}
MASK_NULLABLE = ['ss_store_sk', 'ss_quantity', 'ss_sales_price', 'ss_net_paid']

spark = SparkSession.builder.appName('mask-pipe-resident').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
print('PIPE_RESIDENT_READY', flush=True)

done_runs = set()
while True:
    runs = sorted(p for p in root.glob('run_*') if p.is_dir()
                  and p.name not in done_runs)
    if not runs:
        time.sleep(0.05)
        continue
    run = runs[0]
    label = run.name[4:]
    try:
        expr = EXPR[label.split('_r')[0]]
    except KeyError:
        print('PIPE ' + json.dumps({'label': label, 'error': 'unknown case label',
                                    'batches': 0}), flush=True)
        done_runs.add(run.name)
        continue
    rows = sum1 = sum2 = 0
    processed = set()
    e_work = 0.0
    t_start = None          # set at first file sight = data-arrival clock
    first_batch_lag = None
    last_progress = time.perf_counter()
    RUN_TIMEOUT = 180.0     # no new file and no .done for this long -> abort
    failed = None
    try:
      while True:
        files = sorted(run.glob('*.parquet'))
        pending = [p for p in files if p.name not in processed]
        marker = (run / '.done').exists()
        fail = (run / '.fail')
        if fail.exists():
            failed = fail.read_text()
            break
        if pending:
            last_progress = time.perf_counter()
            if t_start is None:
                t_start = time.perf_counter()
            t0 = time.perf_counter()
            df = spark.read.parquet(*[str(p) for p in pending])
            for c in MASK_NULLABLE:
                df = df.withColumn(c, F.when(F.col(c) != -2, F.col(c)))
            df = df.filter(expr)
            cols = [F.col(c) for c in df.columns]
            final = (df.select(F.xxhash64(*cols).cast('decimal(38,0)').alias('h1'),
                               F.xxhash64(F.lit(9173), *cols).cast('decimal(38,0)').alias('h2'))
                     .agg(F.count('*').alias('n'), F.sum('h1').alias('s1'), F.sum('h2').alias('s2')))
            v = final.first()
            dt = (time.perf_counter() - t0) * 1000
            if first_batch_lag is None:
                first_batch_lag = t0 - t_start
            e_work += dt
            rows += int(v['n'])
            sum1 += int(v['s1'])
            sum2 += int(v['s2'])
            processed.update(p.name for p in pending)
        elif marker:
            break
        else:
            if t_start is not None and time.perf_counter() - last_progress > RUN_TIMEOUT:
                failed = 'watchdog: no progress for %.0fs' % RUN_TIMEOUT
                break
            time.sleep(0.05)
    except Exception as e:
        failed = '%s: %s' % (type(e).__name__, str(e)[:300])
    if failed is not None:
        print('PIPE ' + json.dumps({'label': label, 'error': failed,
                                    'batches': len(processed)}), flush=True)
        done_runs.add(run.name)
        continue
    wall = time.perf_counter() - t_start
    print('PIPE ' + json.dumps({'label': label,
                                'digest': {'rows': str(rows), 'sum1': str(sum1), 'sum2': str(sum2)},
                                'wall_s': round(wall, 2),
                                'first_batch_lag_s': round(first_batch_lag, 3),
                                'e_work_ms': round(e_work, 1),
                                'batches': len(processed)}), flush=True)
    done_runs.add(run.name)
