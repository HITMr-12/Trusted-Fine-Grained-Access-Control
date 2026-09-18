# -*- coding: utf-8 -*-
"""Steady-state MASK e2e benchmark driver (runs on E host).
Prerequisites already running:
  R: python3 mask_e2e_server.py 19041 16        (resident parallel producer)
  E: python3 recv_server.py <root> 19042        (resident receiver)
  E: spark-submit pipeline_e_resident.py <root> (resident consumer, PIPE lines
     to a log file passed to this driver)
Per measured query: send control request to R server -> R produces in parallel
and pushes frames to E receiver -> resident consumer digests -> PIPE line.
e2e = control-sent -> PIPE-line-read. Digests are verified against the aligned
NATIVE digests (records3.jsonl) when --refs is given.

Usage: python3 mask_e2e_bench.py <consumer_log> <out_jsonl> [reps] [refs_jsonl]
"""
import json, socket, sys, time
from pathlib import Path

CASES = {'s001': '173.50', 's01': '143.70', 's10': '88.80', 's477': '27.60',
         's575': '19.46', 's718': '9.97', 's86': '3.12', 's100': 'NONE'}
CASE2FULL = {'s001': 'bench_full_001', 's01': 'bench_full_01', 's10': 'bench_full_1',
             's477': 'bench_full_5', 's575': 'bench_full_06', 's718': 'bench_full_075',
             's86': 'bench_full_09', 's100': 'bench_full_1_0'}
R_HOST, R_PORT = '172.168.22.23', 19041
E_PORT = 19042

log_path, out_path = Path(sys.argv[1]), Path(sys.argv[2])
reps = int(sys.argv[3]) if len(sys.argv) > 3 else 3
rep_start = int(sys.argv[5]) if len(sys.argv) > 5 else 1
refs = {}
if len(sys.argv) > 4:
    for line in open(sys.argv[4]):
        r = json.loads(line)
        if r.get('mode') == 'NATIVE' and not r.get('warmup'):
            refs[r['case']] = r['digest']


def request(label, thr):
    conn = socket.create_connection((R_HOST, R_PORT), timeout=30)
    conn.sendall((json.dumps({'label': label, 'thr': thr,
                              'host': '172.168.22.25', 'port': E_PORT}) + '\n').encode())
    conn.close()


def wait_pipe(label, seen):
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        if log_path.exists():
            for line in open(log_path):
                if line.startswith('PIPE ') and line not in seen:
                    seen.add(line)
                    rec = json.loads(line[5:])
                    if rec['label'] == label:
                        return rec
        time.sleep(0.2)
    raise TimeoutError(label)


seen = set()
# ignore PIPE lines that predate this driver run (stale lines from previous
# runs would otherwise match same-named labels instantly)
if log_path.exists():
    for line in open(log_path):
        if line.startswith('PIPE '):
            seen.add(line)
out = open(out_path, 'a')
if rep_start == 1:
    request('warm', CASES['s01'])      # warm the resident consumer once
    wait_pipe('warm', seen)
    print('warmup done', flush=True)

import random
rng = random.Random(91711)
for rep in range(rep_start, rep_start + reps):
    order = list(CASES)
    rng.shuffle(order)
    for base in order:
        label = '%s_r%d' % (base, rep)
        t0 = time.perf_counter()
        request(label, CASES[base])
        rec = wait_pipe(label, seen)
        e2e = (time.perf_counter() - t0) * 1000
        row = {'case': CASE2FULL[base], 'label': label, 'rep': rep,
               'e2e_ms': round(e2e, 1), 'digest': rec['digest'],
               'consumer_wall_s': rec['wall_s'],
               'first_batch_lag_s': rec['first_batch_lag_s'],
               'e_work_ms': rec['e_work_ms']}
        if CASE2FULL[base] in refs:
            row['equal'] = rec['digest'] == refs[CASE2FULL[base]]
        out.write(json.dumps(row) + '\n')
        out.flush()
        print(json.dumps(row), flush=True)
print('BENCH_DONE', flush=True)
