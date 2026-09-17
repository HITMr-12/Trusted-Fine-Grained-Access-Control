# -*- coding: utf-8 -*-
"""Small interleaved A/B: FGAC (Arrow IPC, port 18833) vs FGAC-FRAMES (Parquet, 18836).
One worker per mode, warmup 1, N interleaved reps on one case."""
import json, os, selectors, socket, subprocess, sys, time
from pathlib import Path

r = Path('/home/lyb/fgac-lab')
d = r / 'deploy/fgac-tpcds-bench'
out = Path(sys.argv[1])
reps = int(sys.argv[2]) if len(sys.argv) > 2 else 5
case_name = sys.argv[3] if len(sys.argv) > 3 else 'bench_full_1_0'
out.mkdir(parents=True, exist_ok=True)
(out / 'events').mkdir(exist_ok=True)

cases = json.loads((d / 'cases.json').read_text())
case = next(c for c in cases if c['name'] == case_name)


def launch(kind, port):
    env = os.environ.copy()
    env.update({'SPARK_CONF_DIR': str(r / 'deploy/fgac-current/conf'),
                'JAVA_HOME': '/usr/lib/jvm/java-11-openjdk-arm64',
                'PYSPARK_PYTHON': '/home/lyb/fgac/venv/bin/python',
                'PYTHONPATH': str(d) + ':' + str(r / 'envs/fgac-python') + ':' + str(r / 'deploy/fgac-current'),
                'HADOOP_USER_NAME': 'lyb',
                'FGAC_FLIGHT_URL': 'grpc://172.168.22.23:%d' % port,
                'SPARK_LOCAL_DIRS': str(r / 'tmp'), 'TMPDIR': str(r / 'tmp')})
    p = subprocess.Popen(['/home/lyb/fgac/spark/spark-3.5.8-bin-hadoop3/bin/spark-submit',
                          '--conf', 'spark.eventLog.dir=file:' + str(out / 'events'),
                          str(d / 'tpcds_worker.py'), kind, 'lyb', str(out)],
                         env=env, cwd=d, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=(out / ('worker-%s.log' % kind)).open('w'), text=True, bufsize=1)
    return p


def read_ready(p, timeout=600):
    deadline = time.monotonic() + timeout
    sel = selectors.DefaultSelector()
    sel.register(p.stdout, selectors.EVENT_READ)
    while time.monotonic() < deadline:
        if not sel.select(max(0, deadline - time.monotonic())):
            raise TimeoutError('ready timeout')
        line = p.stdout.readline()
        if not line:
            raise RuntimeError('worker ended ' + str(p.poll()))
        if line.startswith('BENCH_RESULT '):
            return json.loads(line[len('BENCH_RESULT '):])
    raise TimeoutError()


def call(port, x):
    with socket.create_connection(('127.0.0.1', port), timeout=900) as conn:
        conn.sendall((json.dumps(x) + '\n').encode())
        return json.loads(conn.makefile('r').readline())


procs = {}
ports = {}
for kind, port in (('FGAC', 18833), ('FGAC-FRAMES', 18836)):
    procs[kind] = launch(kind, port)
    ready = read_ready(procs[kind])
    ports[kind] = ready['control_port']
    print('READY', kind, ready['spark'], flush=True)

results = []


def run(kind, rep):
    t0 = time.time()
    row = call(ports[kind], {'op': 'run', 'case': case, 'mode': kind,
                             'id': '%s-%s-%d' % (case_name, kind, rep), 'rep': rep})
    results.append(row)
    (out / 'ab-frames.jsonl').open('a').write(json.dumps(row, ensure_ascii=False) + '\n')
    ok = row.get('ok')
    ms = round(row.get('total_ms', -1)) if ok else -1
    print('RUN', kind, 'rep', rep, 'ok' if ok else 'FAIL', ms, 'ms', flush=True)
    return row


try:
    for kind in ('FGAC', 'FGAC-FRAMES'):
        run(kind, -1)  # warmup
    for rep in range(1, reps + 1):
        for kind in ('FGAC', 'FGAC-FRAMES'):
            run(kind, rep)
finally:
    for kind, p in procs.items():
        try:
            with socket.create_connection(('127.0.0.1', ports[kind]), timeout=60) as conn:
                conn.sendall(json.dumps({'op': 'stop'}).encode() + b'\n')
        except OSError:
            pass
        try:
            p.wait(timeout=120)
        except subprocess.TimeoutExpired:
            p.kill()
print('DONE', flush=True)
