import json, statistics, sys
recs = [json.loads(l) for l in open(sys.argv[1])]
recs = [r for r in recs if not r.get('warmup')]
SEL = {'bench_full_001':'0.1%','bench_full_01':'1%','bench_full_1':'10%','bench_full_5':'47.7%',
       'bench_full_06':'57.5%','bench_full_075':'71.8%','bench_full_09':'86.0%','bench_full_1_0':'100%'}
cases = sorted(set(r['case'] for r in recs), key=lambda c: list(SEL).index(c))
print('%-16s %6s %9s %9s %9s | %8s %8s' % ('case','sel','NATIVE','MASK','FGAC','M vs N','M vs F'))
for c in cases:
    med = {}
    for m in ('NATIVE','MASK','FGAC'):
        med[m] = statistics.median(r['total_ms'] for r in recs if r['case']==c and r['mode']==m)
    print('%-16s %6s %9.0f %9.0f %9.0f | %+7.1f%% %+7.1f%%' % (
        c, SEL[c], med['NATIVE'], med['MASK'], med['FGAC'],
        (med['MASK']-med['NATIVE'])/med['NATIVE']*100,
        (med['MASK']-med['FGAC'])/med['FGAC']*100))
