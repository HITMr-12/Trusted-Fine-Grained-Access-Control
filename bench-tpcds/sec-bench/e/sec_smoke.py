# -*- coding: utf-8 -*-
"""One-case smoke across all six combos before the full SEC bench."""
import json
import socket
import sys

READY = '/home/lyb/fgac-lab/runs/sec-bench-0918/ready'
CASES = '/home/lyb/fgac-lab/runs/sec-bench-0918/cases.json'
case = [c for c in json.load(open(CASES)) if c['name'] == 'bench_full_001'][0]
MODE_OF = {'native': 'NATIVE', 'fgac': 'FGAC', 'mask': 'MASK'}
digests = {}
ok = True
for combo in sys.argv[1:]:
    kind, sec = combo.split('-')
    port = json.load(open('%s/%s.ready' % (READY, kind)))['control_port']
    cmd = {'case': case, 'mode': MODE_OF[kind], 'id': 'smoke-' + combo, 'rep': 1,
           'sec': sec == 'on', 'authorization': 'Bearer bench_full-token'}
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=30) as conn:
            conn.sendall((json.dumps(cmd) + '\n').encode())
            conn.settimeout(300)
            ans = json.loads(conn.makefile('r').readline())
    except Exception as exc:
        print('SMOKE %-10s CALL-ERROR %r' % (combo, exc))
        ok = False
        continue
    if not ans.get('ok'):
        print('SMOKE %-10s FAIL %s' % (combo, ans.get('error', '')[-300:]))
        ok = False
        continue
    d = ans['digest']
    digests[combo] = (d['rows'], d['sum1'], d['sum2'])
    print('SMOKE %-10s OK %8.0fms rows=%s' % (combo, ans['total_ms'], d['rows']))
if len(set(digests.values())) > 1:
    print('SMOKE DIGEST-MISMATCH', json.dumps({k: list(v) for k, v in digests.items()}))
    ok = False
print('SMOKE_DONE ok=%s' % ok)
