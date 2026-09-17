# -*- coding: utf-8 -*-
"""Single-case validation for the FGAC-FRAMES path: launches one worker,
runs bench_full_1_0, prints the result row for digest comparison."""
import json, os, selectors, socket, subprocess, sys, time
from pathlib import Path

r = Path('/home/lyb/fgac-lab')
d = r / 'deploy/fgac-tpcds-bench'
out = Path(sys.argv[1]) if len(sys.argv) > 1 else r / 'runs/frames-validate'
out.mkdir(parents=True, exist_ok=True)
(out / 'events').mkdir(exist_ok=True)
case_name = sys.argv[2] if len(sys.argv) > 2 else 'bench_full_1_0'

cases = json.loads((d / 'cases.json').read_text())
case = next(c for c in cases if c['name'] == case_name)

env = os.environ.copy()
env.update({'SPARK_CONF_DIR': str(r / 'deploy/fgac-current/conf'),
            'JAVA_HOME': '/usr/lib/jvm/java-11-openjdk-arm64',
            'PYSPARK_PYTHON': '/home/lyb/fgac/venv/bin/python',
            'PYTHONPATH': str(d) + ':' + str(r / 'envs/fgac-python') + ':' + str(r / 'deploy/fgac-current'),
            'HADOOP_USER_NAME': 'lyb',
            'FGAC_FLIGHT_URL': 'grpc://172.168.22.23:18836',
            'SPARK_LOCAL_DIRS': str(r / 'tmp'), 'TMPDIR': str(r / 'tmp')})
p = subprocess.Popen(['/home/lyb/fgac/spark/spark-3.5.8-bin-hadoop3/bin/spark-submit',
                      '--conf', 'spark.eventLog.dir=file:' + str(out / 'events'),
                      str(d / 'tpcds_worker.py'), 'FGAC-FRAMES', 'lyb', str(out)],
                     env=env, cwd=d, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                     stderr=(out / 'worker.log').open('w'), text=True, bufsize=1)


def read(timeout=600):
    deadline = time.monotonic() + timeout
    sel = selectors.DefaultSelector()
    sel.register(p.stdout, selectors.EVENT_READ)
    while time.monotonic() < deadline:
        events = sel.select(max(0, deadline - time.monotonic()))
        if not events:
            raise TimeoutError('worker response timeout')
        line = p.stdout.readline()
        if not line:
            raise RuntimeError('worker ended ' + str(p.poll()))
        if line.startswith('BENCH_RESULT '):
            return json.loads(line[len('BENCH_RESULT '):])
    raise TimeoutError()


ready = read()
assert ready.get('ready'), ready
port = ready['control_port']
print('READY', ready['spark'], 'port', port, flush=True)

for rep in (-1, 1):
    with socket.create_connection(('127.0.0.1', port), timeout=900) as conn:
        conn.sendall((json.dumps({'op': 'run', 'case': case, 'mode': 'FGAC-FRAMES',
                                  'id': 'frames-%s-%d' % (case_name, rep), 'rep': rep}) + '\n').encode())
        row = json.loads(conn.makefile('r').readline())
    print('RESULT', json.dumps(row, ensure_ascii=False), flush=True)
    (out / 'frames-validate.jsonl').open('a').write(json.dumps(row, ensure_ascii=False) + '\n')

with socket.create_connection(('127.0.0.1', port), timeout=60) as conn:
    conn.sendall(json.dumps({'op': 'stop'}) .encode() + b'\n')
try:
    p.wait(timeout=120)
except subprocess.TimeoutExpired:
    p.kill()
print('DONE', flush=True)
