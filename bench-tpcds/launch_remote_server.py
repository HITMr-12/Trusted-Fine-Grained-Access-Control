# -*- coding: utf-8 -*-
"""Launcher: start an independent R Flight server with a chosen local[N] parallelism.
Usage: python3 launch_remote_server.py <port> <cores> <log>"""
import os, subprocess, sys
from pathlib import Path

port, cores, log = sys.argv[1], sys.argv[2], sys.argv[3]
raw = Path('/proc/3643583/environ').read_bytes()
env = dict(item.decode().split('=', 1) for item in raw.split(b'\0') if b'=' in item)
env['PYTHONPATH'] = ('/home/lyb/tools/spark/python/lib/pyspark.zip:'
                     '/home/lyb/tools/spark/python/lib/py4j-0.10.9.7-src.zip:'
                     '/data1/lyb/fgac-lab/envs/fgac-python:'
                     '/data1/lyb/fgac-lab/deploy/fgac-current')
env['SPARK_CONF_DIR'] = '/data1/lyb/fgac-lab/deploy/fgac-current/conf'
env['FGAC_FLIGHT_PORT'] = port
env['FGAC_RUN_DIR'] = '/data1/lyb/fgac-lab/runs/tpcds-bench'
env['SPARK_LOCAL_DIRS'] = '/data1/lyb/fgac-lab/tmp'
env['TMPDIR'] = '/data1/lyb/fgac-lab/tmp'
with open(log, 'w') as f:
    subprocess.Popen(['/home/lyb/tools/spark-3.5.8-bin-hadoop3/bin/spark-submit',
                      '--conf', 'spark.master=local[%s]' % cores,
                      'serve_remote_tpcds.py'],
                     cwd='/data1/lyb/fgac-lab/deploy/fgac-current',
                     env=env, stdout=f, stderr=subprocess.STDOUT,
                     start_new_session=True)
print('LAUNCHED port=%s cores=%s' % (port, cores))
