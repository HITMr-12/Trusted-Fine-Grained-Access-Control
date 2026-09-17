# -*- coding: utf-8 -*-
"""zstd storage + decompress-mask-no-reencode e2e producer (precomputed skeleton).

Delivery layout is query-independent: for a given source file the uncompressed
page structure S (page headers + footer, statistics ON, leak ignored) is fixed,
so we precompute a per-file skeleton once and the query-time path does zero
thrift bookkeeping: decode bitmap -> decompress -> memset -> concat -> send.

Modes:
  skeleton                      precompute per-file skeleton.json (query-independent)
  run <label> <thr|NONE>        batch: write delivery_<label>/ per file, self-check
  stream <label> <thr|NONE>     streaming: framed records to stdout (record framing:
                                u16 name_len + name + u64 payload_len + payload),
                                per-file produce timing to stderr; ref digest to
                                result_<label>.json on stdout-end via side file
"""
import json, sys, time, struct
from pathlib import Path
from decimal import Decimal
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import importlib.util

ROOT = Path('/data1/lyb/fgac-lab/runs/mask-e2e')
SRC = Path('/tmp/masklayout_zstd')
REF_TMP = Path('/tmp/masklayout_ref')
COLS = ['ss_item_sk', 'ss_store_sk', 'ss_quantity', 'ss_sales_price', 'ss_net_paid']
WIDTHS = {'ss_item_sk': 8, 'ss_store_sk': 8, 'ss_quantity': 4,
          'ss_sales_price': 4, 'ss_net_paid': 4}
DEC = {'ss_sales_price', 'ss_net_paid'}
SENTINEL = -1
# byte-order views of the decompressed PLAIN payloads (verified vs pyarrow):
# plain ints little-endian; decimal(7,2) physical int32 is BIG-ENDIAN unscaled.
VT = {'ss_item_sk': ('<i8', 8), 'ss_store_sk': ('<i8', 8), 'ss_quantity': ('<i4', 4),
      'ss_sales_price': ('>i4', 4), 'ss_net_paid': ('>i4', 4)}

spec = importlib.util.spec_from_file_location(
    'm1', '/data1/lyb/fgac-lab/runs/mask-phase1/mask_phase1_remote.py')
m1 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m1)
parse_page_header = m1.parse_page_header


def cents_arr(col):
    return pc.cast(pc.cast(pc.multiply(col, 100), pa.decimal128(38, 0), safe=False),
                   pa.int64(), safe=False).to_numpy()


def col_start(cc):
    return (getattr(cc, 'dictionary_page_offset', 0) or 0) \
        or getattr(cc, 'data_page_offset', 0) or cc.file_offset


def walk_chunk(buf, cc):
    """yield (nvals, payload_off, payload_len) for DATA_PAGE_V1 pages of a chunk"""
    pos = col_start(cc)
    cursor = 0
    while cursor < cc.num_values:
        ptype, nvals, comp, hend = parse_page_header(buf, pos)
        assert ptype == 0, 'expect DATA_PAGE_V1 got %d' % ptype
        yield nvals, hend, comp
        cursor += nvals
        pos = hend + comp


def skeleton():
    ROOT.mkdir(parents=True, exist_ok=True)
    REF_TMP.mkdir(exist_ok=True)
    files = sorted(SRC.glob('part-*.parquet'))
    assert files, 'no zstd source in %s' % SRC
    skel = {}
    for f in files:
        t = pq.read_table(f)
        ref = REF_TMP / f.name
        pq.write_table(t, ref, compression='none', use_dictionary=False,
                       write_statistics=True, data_page_version='1.0')
        rbuf = ref.read_bytes()
        md = pq.ParquetFile(pa.BufferReader(rbuf)).metadata
        pages = []
        body_end = 4
        for rg_i in range(md.num_row_groups):
            rg = md.row_group(rg_i)
            row_base = sum(md.row_group(i).num_rows for i in range(rg_i))
            for c_i in range(rg.num_columns):
                cc = rg.column(c_i)
                name = cc.path_in_schema
                w = WIDTHS[name]
                pos = col_start(cc)
                cursor = 0
                while cursor < cc.num_values:
                    ptype, nvals, comp, hend = parse_page_header(rbuf, pos)
                    assert ptype == 0 and comp == nvals * w, (name, comp, nvals, w)
                    pages.append({'col': name, 'rs': row_base + cursor,
                                  'nv': nvals, 'hdr': rbuf[pos:hend].hex(),
                                  'plen': comp})
                    body_end = max(body_end, hend + comp)
                    cursor += nvals
                    pos = hend + comp
        assert rbuf[:4] == b'PAR1' and rbuf[-4:] == b'PAR1'
        skel[f.name] = {'nrows': md.num_rows, 'pages': pages,
                        'footer': rbuf[body_end:].hex()}
        ref.unlink()
        print('skel %s rows=%d pages=%d footer_kb=%d' %
              (f.name, md.num_rows, len(pages), len(skel[f.name]['footer']) // 2 // 1024),
              flush=True)
    REF_TMP.rmdir()
    (ROOT / 'skeleton.json').write_text(json.dumps(skel))
    print('SKELETON_DONE files=%d' % len(skel), flush=True)


def digest_update(acc, name, arr):
    if not len(arr):
        return
    acc[name]['sum'] += int(arr.sum(dtype=np.int64))
    mn, mx = int(arr.min()), int(arr.max())
    acc[name]['min'] = mn if acc[name]['min'] is None else min(acc[name]['min'], mn)
    acc[name]['max'] = mx if acc[name]['max'] is None else max(acc[name]['max'], mx)


_SENT = {c: np.frombuffer(SENTINEL.to_bytes(WIDTHS[c], 'little', signed=True),
                          np.uint8) for c in COLS}


def assemble_file(f, skel_entry, thr_c):
    """Per-file streaming unit: decompress -> bitmap -> mask -> assemble.
    Returns (payload_bytes, keep, views). Memory bounded at one file (~25MB)."""
    sbuf = f.read_bytes()
    md = pq.ParquetFile(pa.BufferReader(sbuf)).metadata
    col_bytes = {}
    for rg_i in range(md.num_row_groups):
        rg = md.row_group(rg_i)
        for c_i in range(rg.num_columns):
            cc = rg.column(c_i)
            name = cc.path_in_schema
            w = WIDTHS[name]
            parts = []
            for nvals, hend, comp in walk_chunk(sbuf, cc):
                parts.append(pa.decompress(sbuf[hend:hend + comp],
                                           decompressed_size=nvals * w,
                                           codec='zstd'))
            col_bytes[name] = b''.join(parts)
    views = {c: np.frombuffer(col_bytes[c], VT[c][0]) for c in COLS}
    price = views['ss_sales_price']
    keep = np.ones(len(price), dtype=bool) if thr_c is None else price >= thr_c
    bad = np.nonzero(~keep)[0]
    for c in COLS:
        w = WIDTHS[c]
        full = bytearray(col_bytes[c])
        if bad.size:
            v = np.frombuffer(full, np.uint8).reshape(-1, w)
            v[bad] = _SENT[c]
        col_bytes[c] = full
    out = bytearray(b'PAR1')
    ap = out.extend
    for p in skel_entry['pages']:
        w = WIDTHS[p['col']]
        ap(bytes.fromhex(p['hdr']))
        ap(bytes(col_bytes[p['col']][p['rs'] * w:(p['rs'] + p['nv']) * w]))
    ap(bytes.fromhex(skel_entry['footer']))
    return bytes(out), keep, views


def _ref_agg(ref, ref_rows):
    return {'rows': ref_rows,
            'cols': {c: {'sum': str(ref[c]['sum']), 'min': ref[c]['min'],
                         'max': ref[c]['max']} for c in COLS}}


def run(label, thr):
    skel = json.loads((ROOT / 'skeleton.json').read_text())
    files = sorted(SRC.glob('part-*.parquet'))
    outdir = ROOT / ('delivery_' + label)
    outdir.mkdir(parents=True, exist_ok=True)
    thr_c = None if thr == 'NONE' else int(Decimal(thr) * 100)
    t_dec = t_dcmp = t_mask = t_asm = t_wr = t_chk = 0.0
    wire = 0
    ref_rows = 0
    ref = {c: {'sum': 0, 'min': None, 'max': None} for c in COLS}
    for f in files:
        t0 = time.perf_counter()
        payload, keep, views = assemble_file(f, skel[f.name], thr_c)
        t_dcmp += (time.perf_counter() - t0) * 1000
        t0 = time.perf_counter()
        for c in COLS:
            digest_update(ref, c, views[c][keep])
        ref_rows += int(keep.sum())
        t_dec += (time.perf_counter() - t0) * 1000
        t0 = time.perf_counter()
        (outdir / f.name).write_bytes(payload)
        t_wr += (time.perf_counter() - t0) * 1000
        wire += len(payload)
        t0 = time.perf_counter()
        if thr_c is not None:
            d = pq.read_table(outdir / f.name)
            dp = cents_arr(d.column('ss_sales_price'))
            assert np.array_equal(dp >= thr_c, keep), \
                'readback filter mismatch %s' % f.name
        t_chk += (time.perf_counter() - t0) * 1000
        print('%s %s rows=%d keep=%d' % (label, f.name, skel[f.name]['nrows'],
                                         int(keep.sum())), flush=True)
    res = {'label': label, 'threshold': thr,
           'R_bitmap_ref_ms': round(t_dec, 1),
           'R_assemble_ms': round(t_dcmp, 1),
           'R_write_ms': round(t_wr, 1),
           'R_readback_check_ms': round(t_chk, 1),
           'R_produce_total_ms': round(t_dec + t_dcmp + t_wr, 1),
           'delivery_bytes': wire, 'ref_agg': _ref_agg(ref, ref_rows)}
    (ROOT / ('result_%s.json' % label)).write_text(json.dumps(res, indent=1))
    print('RUN_DONE ' + json.dumps(res), flush=True)


def stream(label, thr):
    """Streaming mode: framed records on stdout as each file is produced, so a
    transport can drain concurrently with production. Timing per file -> stderr."""
    skel = json.loads((ROOT / 'skeleton.json').read_text())
    files = sorted(SRC.glob('part-*.parquet'))
    thr_c = None if thr == 'NONE' else int(Decimal(thr) * 100)
    out = sys.stdout.buffer
    ref_rows = 0
    ref = {c: {'sum': 0, 'min': None, 'max': None} for c in COLS}
    t_start = time.perf_counter()
    produced = 0
    for f in files:
        t0 = time.perf_counter()
        payload, keep, views = assemble_file(f, skel[f.name], thr_c)
        t_prod = (time.perf_counter() - t0) * 1000
        t0 = time.perf_counter()
        for c in COLS:
            digest_update(ref, c, views[c][keep])
        ref_rows += int(keep.sum())
        t_ref = (time.perf_counter() - t0) * 1000
        nb = f.name.encode()
        out.write(struct.pack('<H', len(nb)) + nb + struct.pack('<Q', len(payload)))
        out.write(payload)
        out.flush()
        produced += len(payload)
        print(json.dumps({'file': f.name, 'produce_ms': round(t_prod, 1),
                          'ref_ms': round(t_ref, 1),
                          'cum_bytes': produced,
                          'cum_wall_s': round(time.perf_counter() - t_start, 2)}),
              file=sys.stderr, flush=True)
    res = {'label': label, 'threshold': thr, 'mode': 'stream',
           'delivery_bytes': produced, 'ref_agg': _ref_agg(ref, ref_rows)}
    (ROOT / ('result_stream_%s.json' % label)).write_text(json.dumps(res, indent=1))
    print('STREAM_DONE ' + json.dumps({'label': label, 'bytes': produced,
                                       'wall_s': round(time.perf_counter() - t_start, 2)}),
          file=sys.stderr, flush=True)


if __name__ == '__main__':
    cmd = sys.argv[1]
    if cmd == 'skeleton':
        skeleton()
    elif cmd == 'stream':
        stream(sys.argv[2], sys.argv[3])
    else:
        run(sys.argv[2], sys.argv[3])
