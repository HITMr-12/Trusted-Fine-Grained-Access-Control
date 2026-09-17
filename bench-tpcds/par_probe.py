# -*- coding: utf-8 -*-
"""Probe: one FGAC worker against a chosen R port; bench_full_1_0, warmup 1 + N reps.
Usage: python3 par_probe.py <outdir> <port> <reps>"""
import json, os, selectors, socket, subprocess, sys, time
from pathlib import Path

r = Path('/home/lyb/fgac-lab')
d = r / 'deploy/fgac-tpcds-bench'
out = Path(sys.argv[1])
port = sys.argv[2]
reps = int(sys.argv[3]) if len(sys.argv) > 3 else 5
out.mkdir(parents=True, exist_ok=True)
(out / 'events').mkdir(exist_ok=True)

cases = json.loads((d / 'cases.json').read_text())
case = next(c for c in cases if c['name'] == 'bench_full_1_0')

env = os.environ.copy()
env.update({'SPARK_CONF_DIR': str(r / 'deploy/fgac-current/conf'),
            'JAVA_HOME': '/usr/lib/jvm/java-11-openjdk-arm64',
            'PYSPARK_PYTHON': '/home/lyb/fgac/venv/bin/python',
            'PYTHONPATH': str(d) + ':' + str(r / 'envs/fgac-python') + ':' + str(r / 'deploy/fgac-current'),
            'HADOOP_USER_NAME': 'lyb',
            'FGAC_FLIGHT_URL': 'grpc://172.168.22.23:%s' % port,
            'SPARK_LOCAL_DIRS': str(r / 'tmp'), 'TMPDIR': str(r / 'tmp')})
p = subprocess.Popen(['/home/lyb/fgac/spark/spark-3.5.8-bin-hadoop3/bin/spark-submit',
                      '--conf', 'spark.eventLog.dir=file:' + str(out / 'events'),
                      str(d / 'tpcds_worker.py'), 'FGAC', 'lyb', str(out)],
                     env=env, cwd=d, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                     stderr=(out / 'worker.log').open('w'), text=True, bufsize=1)


def read(timeout=600):
    deadline = time.monotonic() + timeout
    sel = selectors.DefaultSelector()
    sel.register(p.stdout, selectors.EVENT_READ)
    while time.monotonic() < deadline:
        if not sel.select(max(0, deadline - time.monotonic())):
            raise TimeoutError()
        line = p.stdout.readline()
        if not line:
            raise RuntimeError('worker ended ' + str(p.poll()))
        if line.startswith('BENCH_RESULT '):
            return json.loads(line[len('BENCH_RESULT '):])
    raise TimeoutError()


ready = read()
p.control_port = ready['control_port']
for rep in list(range(0, 1)) + list(range(1, reps + 1)):
    with socket.create_connection(('127.0.0.1', p.control_port), timeout=900) as conn:
        conn.sendall((json.dumps({'op': 'run', 'case': case, 'mode': 'FGAC',
                                  'id': 'par-%s-%d' % (port, rep), 'rep': rep}) + '\n').encode())
        row = json.loads(conn.makefile('r').readline())
    assert row.get('ok'), row
    (out / ('par-%s.jsonl' % port)).open('a').write(json.dumps(row) + '\n')
    print('REP %s total=%.0f prepare=%.0f action=%.0f' % (rep, row['total_ms'],
          row['prepare_ms'], row['action_ms']), flush=True)
with socket.create_connection(('127.0.0.1', p.control_port), timeout=60) as conn:
    conn.sendall(b'{"op":"stop"}\n')
try:
    p.wait(timeout=120)
except subprocess.TimeoutExpired:
    p.kill()
print('DONE', flush=True)
