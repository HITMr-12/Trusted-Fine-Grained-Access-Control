# -*- coding: utf-8 -*-
"""SEC on/off three-mode bench driver. Connects to resident workers' control
ports, runs (mode x sec) per case interleaved, asserts digest equality.

Usage: python3 sec_driver.py --ready-dir <dir> --cases <cases.json> --out <jsonl>
       [--reps 3] [--only native-off,mask-on,...]
Combos: native-off native-on fgac-off fgac-on mask-off mask-on
"""
import argparse
import itertools
import json
import socket
import sys
import time

COMBOS = ['native-off', 'native-on', 'fgac-off', 'fgac-on', 'mask-off', 'mask-on']
MODE_OF = {'native': 'NATIVE', 'fgac': 'FGAC', 'mask': 'MASK'}


def call(port, cmd, timeout=300):
    with socket.create_connection(('127.0.0.1', port), timeout=30) as conn:
        conn.sendall((json.dumps(cmd) + '\n').encode())
        conn.settimeout(timeout)
        line = conn.makefile('r').readline()
    return json.loads(line)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--ready-dir', required=True)
    ap.add_argument('--cases', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--reps', type=int, default=3)
    ap.add_argument('--only', default=None)
    args = ap.parse_args()

    cases = [c for c in json.load(open(args.cases)) if c['name'].startswith('bench_full_')]
    ports = {}
    for kind in ('native', 'fgac', 'mask'):
        ready = json.load(open('%s/%s.ready' % (args.ready_dir, kind)))
        ports[kind] = ready['control_port']
    combos = args.only.split(',') if args.only else COMBOS
    auth = 'Bearer bench_full-token'

    out = open(args.out, 'a')
    seq = len(open(args.out).read().splitlines()) if __import__('os').path.exists(args.out) else 0
    digests = {}
    t0 = time.time()
    rounds = [-1] + list(range(1, args.reps + 1))
    for rep in rounds:
        ordered = list(itertools.product(cases, combos))
        if rep % 2 == 0:
            ordered = [(c, cb) for c, cb in ordered]
        for case, combo in ordered:
            kind, sec = combo.split('-')
            seq += 1
            cmd = {'case': case, 'mode': MODE_OF[kind], 'id': '%s-%s-r%s-%d' % (case['name'], combo, rep, seq),
                   'rep': rep, 'sec': sec == 'on', 'authorization': auth}
            t = time.time()
            ans = call(ports[kind], cmd)
            row = {'sequence': seq, 'case': case['name'], 'combo': combo, 'rep': rep,
                   'warmup': rep < 0, 'driver_wait_ms': (time.time() - t) * 1000}
            if not ans.get('ok'):
                row['ok'] = False
                row['error'] = ans.get('error', 'unknown')[-500:]
                out.write(json.dumps(row) + '\n')
                out.flush()
                print('FAIL', json.dumps(row)[:300], flush=True)
                continue
            digest = ans['digest'] if 'digest' in ans else ans.get('value')
            row.update(ok=True, ms=ans['total_ms'], rows=digest['rows'],
                       sum1=digest['sum1'], sum2=digest['sum2'],
                       e_cpu_ms=ans.get('e_cpu_ms'))
            key = (case['name'], rep)
            digests.setdefault(key, {})[combo] = (digest['rows'], digest['sum1'], digest['sum2'])
            if len(digests[key]) > 1 and len(set(digests[key].values())) != 1:
                row['digest_mismatch'] = digests[key]
            out.write(json.dumps(row) + '\n')
            out.flush()
            print('OK %-16s %-10s r%-2d %8.0fms rows=%s' % (case['name'], combo, rep, row['ms'], row['rows']), flush=True)
    out.close()
    bad = [k for k, v in digests.items() if len(v) > 1 and len(set(v.values())) != 1]
    print('DONE', json.dumps({'ok': not bad, 'mismatch': [str(k) for k in bad],
                              'elapsed_s': round(time.time() - t0, 1)}), flush=True)
    sys.exit(1 if bad else 0)


if __name__ == '__main__':
    main()
