import json, statistics, sys
from pathlib import Path
base = Path(r'D:\Kimi\WorkSpace\可信细粒度访问控制\bench-tpcds\aligned-0917')
recs = [json.loads(l) for l in open(base / 'resident_full.jsonl')]
base3 = [json.loads(l) for l in open(base / 'records3.jsonl')]
base3 = [r for r in base3 if not r.get('warmup')]
SEL = [('bench_full_001','0.1%'),('bench_full_01','1%'),('bench_full_1','10%'),('bench_full_5','47.7%'),
       ('bench_full_06','57.5%'),('bench_full_075','71.8%'),('bench_full_09','86.0%'),('bench_full_1_0','100%')]
print('%-14s %6s | %8s %8s %8s | %14s | %9s %9s | %s' % (
    'case','sel','NATIVE','FGAC','MASK_e2e','M range','vs NATIVE','vs FGAC','digest'))
allpass = True
for c, sel in SEL:
    n = statistics.median(r['total_ms'] for r in base3 if r['case']==c and r['mode']=='NATIVE')
    f = statistics.median(r['total_ms'] for r in base3 if r['case']==c and r['mode']=='FGAC')
    m = [r for r in recs if r['case']==c]
    mm = statistics.median(r['e2e_ms'] for r in m)
    lo, hi = min(r['e2e_ms'] for r in m), max(r['e2e_ms'] for r in m)
    eq = all(r.get('equal') for r in m)
    vs_n = (mm-n)/n*100; vs_f = (mm-f)/f*100
    goal = (sel in ('57.5%','71.8%','86.0%','100%') and vs_f < 0 and vs_n <= 10)
    allpass &= eq
    print('%-14s %6s | %8.0f %8.0f %8.0f | %5.0f-%5.0f | %+8.1f%% %+8.1f%% | %s %s' % (
        c, sel, n, f, mm, lo, hi, vs_n, vs_f, 'EQ' if eq else 'MISMATCH',
        'GOAL✓' if goal else ''))
print('ALL_DIGEST_EQUAL:', allpass)
