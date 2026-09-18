# -*- coding: utf-8 -*-
"""Aggregate MASK resident reps (v2/v3/v4) + NATIVE/FGAC refs (records3) into
per-case medians for the §14 reliability report."""
import json, statistics as st
from pathlib import Path

BASE = Path(__file__).parent
CASES = ['s001', 's01', 's10', 's477', 's575', 's718', 's86', 's100']

mask = {}
for f in ['resident_v2.jsonl', 'resident_v3.jsonl', 'resident_v4.jsonl']:
    p = BASE / f
    if not p.exists():
        continue
    for line in p.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if not r.get('equal'):
            print('UNEQUAL!', r['label'])
        mask.setdefault(r['label'].split('_r')[0], []).append((r['label'], r['e2e_ms'], f))

refs = {}
for line in (BASE / 'records3.jsonl').read_text(encoding='utf-8').splitlines():
    r = json.loads(line)
    if r.get('warmup'):
        continue
    key = (r['case'], r['mode'])
    refs.setdefault(key, []).append(r['total_ms'])

# map case -> short label
CASEMAP = {'bench_full_001': 's001', 'bench_full_01': 's01', 'bench_full_1': 's10',
           'bench_full_5': 's477', 'bench_full_06': 's575', 'bench_full_075': 's718',
           'bench_full_09': 's86', 'bench_full_1_0': 's100'}

print('case  | MASK med (n, min-max)      | NATIVE med (n) | FGAC med (n) | vs FGAC | vs NATIVE')
for c in CASES:
    mv = [v for _, v, _ in mask.get(c, [])]
    case = [k for k, v in CASEMAP.items() if v == c][0]
    nv = refs.get((case, 'NATIVE'), [])
    fv = refs.get((case, 'FGAC'), [])
    mm = st.median(mv) if mv else float('nan')
    nm = st.median(nv) if nv else float('nan')
    fm = st.median(fv) if fv else float('nan')
    print('%-5s | %7.0f (n=%d %.0f-%.0f) | %7.0f (n=%d)   | %7.0f (n=%d)   | %+6.1f%% | %+6.1f%%'
          % (c, mm, len(mv), min(mv), max(mv), nm, len(nv), fm, len(fv),
             (mm / fm - 1) * 100, (mm / nm - 1) * 100))

print()
print('MASK per-rep detail (label, e2e_ms, file):')
for c in CASES:
    for label, v, f in sorted(mask.get(c, [])):
        print('  %-10s %7.0f  %s' % (label, v, f))
