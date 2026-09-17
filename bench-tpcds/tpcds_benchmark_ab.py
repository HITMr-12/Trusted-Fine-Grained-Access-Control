# -*- coding: utf-8 -*-
"""TPC-DS 压缩 A/B 主控：NATIVE 锚 + FGAC(NONE/LZ4/ZSTD) 同 R Spark 三端口对照。
用法: python3 tpcds_benchmark_ab.py <outdir> [--smoke] [--warmup N] [--reps N] [--seed S]
"""
import itertools, json, os, random, selectors, subprocess, sys, time, traceback, socket, threading, urllib.request, urllib.parse
from pathlib import Path

r = Path('/home/lyb/fgac-lab')
d = r / 'deploy/fgac-tpcds-bench'
out = Path(sys.argv[1])
smoke = '--smoke' in sys.argv
def arg(flag, default):
    return int(sys.argv[sys.argv.index(flag) + 1]) if flag in sys.argv else default
warmup_cycles = arg('--warmup', 20)
reps = arg('--reps', 6)
seed = arg('--seed', 91620)
out.mkdir(parents=True, exist_ok=True)
(out / 'events').mkdir(exist_ok=True)

FGAC_MODES = ['FGAC', 'FGAC-LZ4', 'FGAC-ZSTD']
ALL_MODES = ['NATIVE'] + FGAC_MODES
workers = {}
records = []
readies = []
manifest = {'ok': False}


def snapshot():
    c = json.loads((r / 'deploy/polaris-credentials.json').read_text())
    base = 'http://172.168.22.23:18182/api/catalog/v1'
    data = urllib.parse.urlencode({'grant_type': 'client_credentials', 'client_id': c['client_id'],
                                   'client_secret': c['client_secret'],
                                   'scope': 'PRINCIPAL_ROLE:ALL'}).encode()
    with urllib.request.urlopen(urllib.request.Request(base + '/oauth/tokens', data=data), timeout=15) as f:
        token = json.load(f)['access_token']
    with urllib.request.urlopen(urllib.request.Request(base + '/fgac/namespaces/tpcds/tables/store_sales',
                                                      headers={'Authorization': 'Bearer ' + token}), timeout=15) as f:
        meta = json.load(f)
    return {'snapshot': meta['metadata']['current-snapshot-id'],
            'metadata_location': meta['metadata-location'], 'time': time.time()}


def save(name, x):
    (out / name).write_text(json.dumps(x, indent=2, default=str))


def read(p, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        sel = selectors.DefaultSelector()
        sel.register(p.stdout, selectors.EVENT_READ)
        events = sel.select(max(0, deadline - time.monotonic()))
        sel.close()
        if not events:
            raise TimeoutError('Worker response timeout')
        line = p.stdout.readline()
        if not line:
            raise RuntimeError('Worker ended with ' + str(p.poll()))
        if line.startswith('BENCH_RESULT '):
            return json.loads(line[len('BENCH_RESULT '):])
    raise TimeoutError()


def call(p, x):
    with socket.create_connection(('127.0.0.1', p.control_port), timeout=300) as conn:
        conn.sendall((json.dumps(x) + '\n').encode())
        return json.loads(conn.makefile('r').readline())


def launch(kind, user):
    conf = {'NATIVE': 'native-conf'}.get(kind, 'conf')
    env = os.environ.copy()
    env.update({'SPARK_CONF_DIR': str(r / 'deploy/fgac-current' / conf),
                'JAVA_HOME': '/usr/lib/jvm/java-11-openjdk-arm64',
                'PYSPARK_PYTHON': '/home/lyb/fgac/venv/bin/python',
                'PYTHONPATH': str(d) + ':' + str(r / 'envs/fgac-python') + ':' + str(r / 'deploy/fgac-current'),
                'HADOOP_USER_NAME': user,
                'SPARK_LOCAL_DIRS': str(r / 'tmp'), 'TMPDIR': str(r / 'tmp')})
    p = subprocess.Popen(['/home/lyb/fgac/spark/spark-3.5.8-bin-hadoop3/bin/spark-submit',
                          '--conf', 'spark.eventLog.dir=file:' + str(out / 'events'),
                          str(d / 'tpcds_worker.py'), kind, user, str(out)],
                         env=env, cwd=d, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=(out / (kind + '-' + user + '.log')).open('w'),
                         text=True, bufsize=1)
    workers[(kind, user)] = p
    ready = read(p)
    assert ready.get('ready'), ready
    p.control_port = ready['control_port']
    readies.append(ready)
    save('workers.json', readies)
    print('ready', kind, user, flush=True)

    def drain():
        with (out / (kind + '-' + user + '-stdout.log')).open('w') as f:
            for line in p.stdout:
                f.write(line)
    threading.Thread(target=drain, daemon=True).start()


def policies():
    scope = {}
    exec((r / 'deploy/configure_authz.py').read_text().split('users =')[0], scope)
    return scope['api']('GET', '/service/public/v2/api/policy?serviceName=fgac_bench_0915')


try:
    save('snapshot-before.json', snapshot())
    before_policies = policies()
    save('ranger-policies-before.json', before_policies)
    cases = json.loads((d / 'cases.json').read_text())
    save('cases.json', cases)
    save('protocol.json', {'modes': ALL_MODES, 'warmup_cycles': warmup_cycles,
                           'formal_repetitions': reps, 'seed': seed,
                           'wire': 'Arrow IPC over gRPC; LZ4/ZSTD via IpcWriteOptions, client transparent',
                           'compute_budget': 'identical local[2] E workers; one R Spark serves 18833/18834/18835 sharing one scan slot',
                           'smoke': smoke})
    for kind, user in [(m, 'lyb') for m in FGAC_MODES] + \
                      [('NATIVE', u) for u in ('alice', 'bob', 'bench_full')]:
        launch(kind, user)

    refs = {}
    for u in ('alice', 'bob', 'bench_full'):
        ref_path = r / 'runs/tpcds-bench' / ('native-%s.jsonl' % u)
        for line in ref_path.read_text().splitlines():
            x = json.loads(line)
            if x.get("ok"):
                refs[x['case']] = {'digest': {'rows': str(x['rows']), 'sum1': x['sum1'], 'sum2': x['sum2']},
                                   'schema': x['schema']}
    assert len(refs) == len(cases), (len(refs), len(cases))

    if not smoke:
        bad = dict(cases[0], principal='invalid_identity')
        result = call(workers[('FGAC', 'lyb')], {'id': 'negative-fgac', 'case': bad, 'mode': 'FGAC', 'rep': -99})
        assert not result.get('ok') and 'Unauthenticated' in result['error'], result
        save('negative-controls.json', [result])

    warm_rng = random.Random(seed + 1)
    for i in range(1 if smoke else warmup_cycles):
        actors = list(workers)
        warm_rng.shuffle(actors)
        for kind, user in actors:
            own = [c for c in cases if c['principal'] == user] if kind == 'NATIVE' else cases
            c = own[i % len(own)]
            result = call(workers[(kind, user)], {'id': 'warm-%s-%s-%d' % (kind, user, i),
                                                  'case': c, 'mode': kind, 'rep': i - warmup_cycles})
            assert result.get('ok') and result['digest'] == refs[c['name']]['digest'] and result['schema'] == refs[c['name']]['schema'], result
            records.append(result)
            with (out / 'records.jsonl').open('a') as f:
                f.write(json.dumps(result) + '\n')
        save('progress.json', {'phase': 'warmup', 'cycle': i, 'records': len(records), 'time': time.time()})
        if i % 10 == 0:
            print('warmup cycle', i, flush=True)

    rng = random.Random(seed)
    per = [list(p) for p in itertools.permutations(ALL_MODES)]
    for rep in (range(1) if smoke else range(reps)):
        cycle = cases[:]
        rng.shuffle(cycle)
        for c in cycle:
            order = per[rep % len(per)]
            rng.shuffle(order)
            pair = []
            for mode in order:
                worker = workers[(mode, c['principal'] if mode == 'NATIVE' else 'lyb')]
                result = call(worker, {'id': '%s-%d-%s' % (c['name'], rep, mode), 'case': c,
                                       'mode': mode, 'rep': rep, 'capture_plan': rep == 0 and mode == 'FGAC'})
                assert result.get('ok'), result
                records.append(result)
                pair.append(result)
                with (out / 'records.jsonl').open('a') as f:
                    f.write(json.dumps(result) + '\n')
            assert all(x['digest'] == pair[0]['digest'] and x['schema'] == pair[0]['schema'] for x in pair), ('RESULT MISMATCH', c['name'], rep)
            print(json.dumps({'case': c['name'], 'rep': rep,
                              **{x['mode']: round(x['total_ms'], 2) for x in pair}}), flush=True)
        save('progress.json', {'phase': 'formal', 'rep': rep, 'records': len(records), 'last': time.time()})

    save('snapshot-after.json', snapshot())
    after_policies = policies()
    save('ranger-policies-after.json', after_policies)
    assert before_policies == after_policies
    manifest = {'ok': True, 'records': len(records),
                'formal_records': sum(not x['warmup'] for x in records),
                'cases': len(cases), 'all_paired_results_equal': True}
except Exception:
    manifest['error'] = traceback.format_exc()
    print(manifest['error'], flush=True)
    raise
finally:
    save('done.json', manifest)
    for p in workers.values():
        try:
            with socket.create_connection(('127.0.0.1', p.control_port), timeout=5) as conn:
                conn.sendall(b'{"op":"stop"}\n')
        except Exception:
            pass
    for p in workers.values():
        try:
            p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            p.terminate()
