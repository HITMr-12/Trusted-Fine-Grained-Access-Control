import json, statistics, sys
recs = [json.loads(l) for l in open(sys.argv[1])]
recs = [r for r in recs if not r.get('warmup')]
SEL = {'bench_full_001':'0.1%','bench_full_01':'1%','bench_full_1':'10%','bench_full_5':'47.7%',
       'bench_full_06':'57.5%','bench_full_075':'71.8%','bench_full_09':'86.0%','bench_full_1_0':'100%'}
cases = sorted(recs and set(r['case'] for r in recs), key=lambda c: list(SEL).index(c))
print('%-16s %6s %10s %10s %8s | %10s %10s' % ('case','sel','NATIVE_ms','MASK_ms','diff','N_action','M_action'))
for c in cases:
    n = [r for r in recs if r['case']==c and r['mode']=='NATIVE']
    m = [r for r in recs if r['case']==c and r['mode']=='MASK']
    nt, mt = statistics.median(r['total_ms'] for r in n), statistics.median(r['total_ms'] for r in m)
    na, ma = statistics.median(r['action_ms'] for r in n), statistics.median(r['action_ms'] for r in m)
    print('%-16s %6s %10.0f %10.0f %+7.1f%% | %10.0f %10.0f' % (c, SEL[c], nt, mt, (mt-nt)/nt*100, na, ma))
    for r in sorted(n+m, key=lambda r:(r['mode'],r['rep'])):
        print('   %s rep%d total=%.0f action=%.0f rows=%s' % (r['mode'], r['rep'], r['total_ms'], r['action_ms'], r['digest']['rows']))
