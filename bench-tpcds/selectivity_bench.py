# -*- coding: utf-8 -*-
"""Selectivity baseline: FGAC (current best) vs NATIVE vs INLINE across the
bench_full business-predicate ladder, high-selectivity range densified."""
import itertools, json, os, random, selectors, socket, subprocess, sys, time
from pathlib import Path

r = Path('/home/lyb/fgac-lab')
d = r / 'deploy/fgac-tpcds-bench'
out = Path(sys.argv[1])
reps = int(sys.argv[2]) if len(sys.argv) > 2 else 8
warmup_cycles = int(sys.argv[3]) if len(sys.argv) > 3 else 1
cases_path = Path(sys.argv[4]) if len(sys.argv) > 4 else d / 'cases_selectivity.json'
seed = 91711
out.mkdir(parents=True, exist_ok=True)
(out / 'events').mkdir(exist_ok=True)

cases = [c for c in json.loads(Path(cases_path).read_text()) if c['principal'] == 'bench_full']
assert len(cases) >= 5, [c['name'] for c in cases]

refs = {}
ref_file = r / 'runs/tpcds-bench/native-bench_full.jsonl'
if ref_file.exists():
    for line in ref_file.read_text().splitlines():
        x = json.loads(line)
        if x.get('ok'):
            refs[x['case']] = {'digest': {'rows': str(x['rows']), 'sum1': x['sum1'], 'sum2': x['sum2']}}

workers = {}
records = []


def launch(kind, user):
    env = os.environ.copy()
    env.update({'SPARK_CONF_DIR': str(r / 'deploy/fgac-current/conf'),
                'JAVA_HOME': '/usr/lib/jvm/java-11-openjdk-arm64',
                'PYSPARK_PYTHON': '/home/lyb/fgac/venv/bin/python',
                'PYTHONPATH': str(d) + ':' + str(r / 'envs/fgac-python') + ':' + str(r / 'deploy/fgac-current'),
                'HADOOP_USER_NAME': user,
                'FGAC_FLIGHT_URL': 'grpc://172.168.22.23:18833',
                'SPARK_LOCAL_DIRS': str(r / 'tmp'), 'TMPDIR': str(r / 'tmp')})
    p = subprocess.Popen(['/home/lyb/fgac/spark/spark-3.5.8-bin-hadoop3/bin/spark-submit',
                          '--conf', 'spark.eventLog.dir=file:' + str(out / 'events'),
                          str(d / 'tpcds_worker.py'), kind, user, str(out)],
                         env=env, cwd=d, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=(out / ('worker-%s-%s.log' % (kind, user))).open('w'),
                         text=True, bufsize=1)
    workers[(kind, user)] = p
    deadline = time.monotonic() + 600
    sel = selectors.DefaultSelector()
    sel.register(p.stdout, selectors.EVENT_READ)
    ready = None
    while time.monotonic() < deadline:
        if not sel.select(max(0, deadline - time.monotonic())):
            raise TimeoutError('ready timeout ' + kind)
        line = p.stdout.readline()
        if not line:
            raise RuntimeError('worker ended ' + str(p.poll()))
        if line.startswith('BENCH_RESULT '):
            ready = json.loads(line[len('BENCH_RESULT '):])
            break
    assert ready.get('ready'), ready
    p.control_port = ready['control_port']
    print('READY', kind, user, flush=True)


def call(port, x):
    with socket.create_connection(('127.0.0.1', port), timeout=900) as conn:
        conn.sendall((json.dumps(x) + '\n').encode())
        return json.loads(conn.makefile('r').readline())


def run(case, mode, rep):
    user = case['principal'] if mode == 'NATIVE' else 'lyb'
    row = call(workers[(mode, user)].control_port,
               {'id': '%s-%d-%s' % (case['name'], rep, mode), 'case': case, 'mode': mode, 'rep': rep})
    assert row.get('ok'), row
    if case['name'] in refs:
        assert row['digest'] == refs[case['name']]['digest'], \
            ('DIGEST MISMATCH vs native ref', case['name'], mode)
    records.append(row)
    with (out / 'records.jsonl').open('a') as f:
        f.write(json.dumps(row) + '\n')
    return row


manifest = {'ok': False}
try:
    for kind, user in [('FGAC', 'lyb'), ('INLINE', 'lyb'), ('NATIVE', 'bench_full')]:
        launch(kind, user)

    warm_rng = random.Random(seed + 1)
    for i in range(warmup_cycles):
        actors = list(workers)
        warm_rng.shuffle(actors)
        for kind, user in actors:
            c = cases[i % len(cases)]
            run(c, kind, i - warmup_cycles)
        print('warmup cycle', i, flush=True)

    per = list(itertools.permutations(['NATIVE', 'FGAC', 'INLINE']))
    rng = random.Random(seed)
    for rep in range(1, reps + 1):
        cycle = cases[:]
        rng.shuffle(cycle)
        for c in cycle:
            order = per[rep % 6]
            pair = [run(c, m, rep) for m in order]
            assert all(x['digest'] == pair[0]['digest'] for x in pair), \
                ('CROSS-MODE MISMATCH', c['name'], rep)
            print(json.dumps({'case': c['name'], 'rep': rep,
                              **{x['mode']: round(x['total_ms'], 1) for x in pair}}), flush=True)
        (out / 'progress.json').write_text(json.dumps({'rep': rep, 'records': len(records)}))
    manifest = {'ok': True, 'records': len(records), 'cases': len(cases), 'reps': reps}
except Exception:
    import traceback
    manifest['error'] = traceback.format_exc()
    print(manifest['error'], flush=True)
finally:
    (out / 'done.json').write_text(json.dumps(manifest))
    for p in workers.values():
        try:
            with socket.create_connection(('127.0.0.1', p.control_port), timeout=5) as conn:
                conn.sendall(b'{"op":"stop"}\n')
        except Exception:
            pass
    for p in workers.values():
        try:
            p.wait(timeout=60)
        except subprocess.TimeoutExpired:
            p.kill()
print('DONE', json.dumps(manifest), flush=True)
