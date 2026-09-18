"""MASK v3 E-side scan: v2 governance/SEC/defensive designs intact.

Removed low-value costs (see bench report):
- one Spark job per delivered file  -> micro-batched jobs (BATCH files/job)
- 1 MiB reception chunks            -> 8 MiB chunks

Unchanged: pull authorization, per-file sha256 + explicit commit gating,
tentative compute discarded unless the commit validates, bounded in-flight
computation (one executing batch + at most one queued), schema consistency
checks, byte-budgeted R-side production.
"""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import os
import time

from mask_protocol import LEASE_SECONDS
from mask_reader import read_parts
from mask_recv import fetch

BATCH = int(os.environ.get('MASK3_BATCH_FILES', '8'))
CHUNK = int(os.environ.get('MASK3_CHUNK_BYTES', str(8 * 1024 * 1024)))


def scan_digest(spark, root, request, business, *, host, port, ca=None, sec=True):
    from pyspark.sql import functions as F
    start = time.monotonic()
    deadline = start + LEASE_SECONDS
    job = 'mask3-' + request['request_id']
    pending = deque()
    staged = []
    sums = {'rows': 0, 'sum1': 0, 'sum2': 0}
    has_values = False
    delivered_schema = None
    files_seen = 0
    first = None

    def compute(paths):
        spark.sparkContext.setJobGroup(job, 'Tentative MASK v3 digest', interruptOnCancel=True)
        try:
            df = read_parts(spark, paths, request['columns'], business)
            cols = [F.col(c) for c in df.columns]
            value = (df.select(F.xxhash64(*cols).cast('decimal(38,0)').alias('h1'),
                               F.xxhash64(F.lit(9173), *cols).cast('decimal(38,0)').alias('h2'))
                     .agg(F.count('*').alias('rows'), F.sum('h1').alias('sum1'),
                          F.sum('h2').alias('sum2')).first().asDict())
            return value, [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
        finally:
            spark.sparkContext.setLocalProperty('spark.jobGroup.id', None)

    def collect_one():
        nonlocal has_values, delivered_schema
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('MASK v3 consumption lease expired')
        value, schema = pending.popleft().result(timeout=remaining)
        if delivered_schema is not None and delivered_schema != schema:
            raise ValueError('inconsistent file schema')
        delivered_schema = schema
        sums['rows'] += int(value['rows'])
        for key in ('sum1', 'sum2'):
            if value[key] is not None:
                sums[key] += int(value[key])
                has_values = True

    executor = ThreadPoolExecutor(max_workers=1)
    try:
        def flush_batch():
            if not staged:
                return
            # One executing and at most one pending Spark batch computation.
            if len(pending) >= 2:
                collect_one()
            pending.append(executor.submit(compute, list(staged)))
            staged.clear()

        def on_file(path):
            nonlocal first, files_seen
            if first is None:
                first = time.monotonic()
            files_seen += 1
            staged.append(str(path))
            if len(staged) >= BATCH:
                flush_batch()

        directory, manifest = fetch(root, request, host=host, port=port, ca=ca, sec=sec,
                                    on_file=on_file, chunk_bytes=CHUNK)
        # fetch() only returns after every hash and the explicit commit match.
        flush_batch()
        while pending:
            collect_one()
        if delivered_schema is None:
            raise ValueError('no data files were committed')
        return {'digest': {k: str(v) if k == 'rows' or has_values else 'None' for k, v in sums.items()},
                'schema': delivered_schema, 'delivery_directory': str(directory),
                'files': manifest['file_count'],
                'batches': (files_seen + BATCH - 1) // BATCH if files_seen else 0,
                'first_file_ms': (first - start) * 1000 if first is not None else None}
    finally:
        # On stream/commit failure all tentative aggregates are discarded.
        for future in pending:
            future.cancel()
        spark.sparkContext.cancelJobGroup(job)
        executor.shutdown(wait=True, cancel_futures=True)
