# -*- coding: utf-8 -*-
"""Resident R-side masking producer service.
Loads skeleton once, then serves queries over TCP: each request line is
  {"label": str, "thr": "173.50"|"NONE", "host": E_IP, "port": E_port}
It produces all 32 files in PARALLEL (process pool; zstd decompress + bitmap +
memset + assemble per file) and pushes framed records (u16 nlen + name +
u64 plen + payload) to the E receiver as they complete. Per-request timing and
merged ref digest are appended to server.log.

Run: python3 mask_e2e_server.py [port] [workers]
"""
import json, socket, struct, sys, time
from concurrent.futures import ProcessPoolExecutor
from decimal import Decimal
from pathlib import Path
import importlib.util

spec = importlib.util.spec_from_file_location(
    'prod', '/data1/lyb/fgac-lab/runs/mask-e2e/mask_e2e_producer.py')
prod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prod)

ROOT = prod.ROOT
SRC = prod.SRC
COLS = prod.COLS
SKEL = json.loads((ROOT / 'skeleton.json').read_text())
FILES = [f.name for f in sorted(SRC.glob('part-*.parquet'))]


def produce_one(args):
    """Worker: assemble one file. Returns (name, payload, ref_part, keep_rows)."""
    fname, thr_c = args
    payload, keep, views = prod.assemble_file(SRC / fname, SKEL[fname], thr_c)
    ref = {}
    for c in COLS:
        arr = views[c][keep]
        ref[c] = {'sum': int(arr.sum()) if len(arr) else 0,
                  'min': int(arr.min()) if len(arr) else None,
                  'max': int(arr.max()) if len(arr) else None}
    return fname, payload, ref, int(keep.sum())


def merge_ref(acc, part):
    for c in COLS:
        a, p = acc[c], part[c]
        a['sum'] += p['sum']
        a['min'] = p['min'] if a['min'] is None else (a['min'] if p['min'] is None else min(a['min'], p['min']))
        a['max'] = p['max'] if a['max'] is None else (a['max'] if p['max'] is None else max(a['max'], p['max']))


def serve(port, workers):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('0.0.0.0', port))
    srv.listen(4)
    pool = ProcessPoolExecutor(max_workers=workers)  # resident pool, fork-inherits SKEL
    # force ALL workers to fork NOW, before any request socket exists —
    # lazy spawn during a request would let children inherit the outgoing
    # socket fd, keeping the TCP connection open after the parent closes it
    # (observed: EOF never delivered to the receiver -> .done never written).
    list(pool.map(int, range(workers)))
    print('R_SERVER_READY port=%d workers=%d files=%d' % (port, workers, len(FILES)), flush=True)
    log = open(ROOT / 'server.log', 'a')
    while True:
        conn, _ = srv.accept()
        try:
            line = conn.makefile('r').readline()
            req = json.loads(line)
            conn.close()
            label, thr = req['label'], req['thr']
            thr_c = None if thr == 'NONE' else int(Decimal(thr) * 100)
            out = socket.create_connection((req['host'], req['port']), timeout=30)
            mb = ('@run@' + label).encode()
            out.sendall(struct.pack('<H', len(mb)) + mb + struct.pack('<Q', 0))
            t0 = time.perf_counter()
            ref = {c: {'sum': 0, 'min': None, 'max': None} for c in COLS}
            ref_rows = 0
            sent = 0
            for fname, payload, part, krows in pool.map(
                    produce_one, [(f, thr_c) for f in FILES], chunksize=1):
                nb = fname.encode()
                out.sendall(struct.pack('<H', len(nb)) + nb
                            + struct.pack('<Q', len(payload)) + payload)
                sent += len(payload)
                merge_ref(ref, part)
                ref_rows += krows
            out.close()
            dt = time.perf_counter() - t0
            rec = {'label': label, 'thr': thr, 'bytes': sent, 'rows': ref_rows,
                   'produce_push_wall_s': round(dt, 2),
                   'ref_agg': {'rows': ref_rows, 'cols': {c: {'sum': str(ref[c]['sum']),
                               'min': ref[c]['min'], 'max': ref[c]['max']} for c in COLS}}}
            log.write(json.dumps(rec) + '\n')
            log.flush()
            print('R_SERVED ' + json.dumps({'label': label, 'wall_s': round(dt, 2),
                                            'bytes': sent}), flush=True)
        except Exception as e:
            print('R_ERROR %r' % e, flush=True)


if __name__ == '__main__':
    serve(int(sys.argv[1]) if len(sys.argv) > 1 else 19041,
          int(sys.argv[2]) if len(sys.argv) > 2 else 16)
