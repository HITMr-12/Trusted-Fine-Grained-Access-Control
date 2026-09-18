# -*- coding: utf-8 -*-
"""One-off: build the trusted NATIVE per-file sha256 manifest over the working
S3A path (same channel the NATIVE queries use). Run once as trusted prep."""
import hashlib
import json

from pyspark.sql import SparkSession

s = SparkSession.builder.appName('sec-native-manifest').getOrCreate()
s.sparkContext.setLogLevel('ERROR')
files = [r[0] for r in s.sql('SELECT file_path FROM fgac.tpcds.store_sales.files').collect()]
files = sorted(files)
got = (s.sparkContext.binaryFiles(','.join(files))
       .map(lambda kv: (kv[0].rsplit('/', 1)[-1], len(kv[1]), hashlib.sha256(kv[1]).hexdigest()))
       .collect())
manifest = {'files': [{'name': n, 'size': z, 'sha256': h} for n, z, h in got]}
with open('/home/lyb/fgac-lab/runs/sec-bench-0918/native-manifest.json', 'w') as f:
    json.dump(manifest, f)
print('NATIVE_MANIFEST_OK', len(got), sum(z for _, z, _ in got))
s.stop()
