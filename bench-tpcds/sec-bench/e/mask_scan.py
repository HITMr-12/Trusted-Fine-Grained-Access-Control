"""Bounded E-side tentative digest pipeline; success requires global commit.

Only the benchmark's count/two-hash sums are decomposed across files. This is
not an implementation of arbitrary joins/order/limit per-file execution.
"""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import time

from mask_protocol import LEASE_SECONDS
from mask_reader import read_parts
from mask_recv import fetch


def scan_digest(spark, root, request, business, *, host, port, ca=None, sec=True):
    from pyspark.sql import functions as F
    start = time.monotonic()
    deadline = start + LEASE_SECONDS
    job = 'mask-' + request['request_id']
    pending = deque()
    sums = {'rows': 0, 'sum1': 0, 'sum2': 0}
    has_values = False
    delivered_schema = None
    first = None

    def compute(path):
        spark.sparkContext.setJobGroup(job, 'Tentative MASK digest', interruptOnCancel=True)
        try:
            df = read_parts(spark, [path], request['columns'], business)
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
            raise TimeoutError('MASK consumption lease expired')
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
        def on_file(path):
            nonlocal first
            if first is None:
                first = time.monotonic()
            # One executing and at most one pending Spark file calculation.
            if len(pending) >= 2:
                collect_one()
            pending.append(executor.submit(compute, path))
        directory, manifest = fetch(root, request, host=host, port=port, ca=ca, sec=sec, on_file=on_file)
        # fetch() only returns after every hash and the explicit commit match.
        while pending:
            collect_one()
        if delivered_schema is None:
            raise ValueError('no data files were committed')
        return {'digest': {k: str(v) if k == 'rows' or has_values else 'None' for k, v in sums.items()},
                'schema': delivered_schema, 'delivery_directory': str(directory),
                'files': manifest['file_count'],
                'first_file_ms': (first - start) * 1000 if first is not None else None}
    finally:
        # On stream/commit failure all tentative aggregates are discarded.
        for future in pending:
            future.cancel()
        spark.sparkContext.cancelJobGroup(job)
        executor.shutdown(wait=True, cancel_futures=True)
