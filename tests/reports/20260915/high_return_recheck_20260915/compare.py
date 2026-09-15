import json,random,statistics
from pathlib import Path
p=Path(__file__).resolve().parent
read=lambda path:json.loads(path.read_text(encoding='utf-8-sig'))
old={x['case']:x for x in read(p.parent/'native_baseline_20260915/summary.json')}
current=read(p/'summary.json');pairs=read(p/'pairs.json');rows=read(p/'verified-records.json')
def quantile(v,f):
    a=sorted(v);pos=(len(a)-1)*f;i=int(pos)
    return a[i]+(a[min(i+1,len(a)-1)]-a[i])*(pos-i)
def block_ci(xs):
    rng=random.Random(91502);vals=[]
    for _ in range(10000):
        indices=[(start+j)%60 for start in [rng.randrange(60) for _ in range(12)] for j in range(5)]
        vals.append(statistics.mean(xs[i] for i in indices))
    return [quantile(vals,.025),quantile(vals,.975)]
comparison=[]
for x in current:
    c=x['case'];a=old[c];ps=sorted([q for q in pairs if q['case']==c],key=lambda q:q['rep'])
    ratios=[q['FGAC_over_NATIVE_minus1'] for q in ps]
    entry={'case':c,'previous_degradation':a['FGAC_over_NATIVE_mean'],'current_degradation':x['FGAC_over_NATIVE_mean'],'current_block_ci95':block_ci(ratios)}
    entry['latency_changes']={m:{'previous_median_ms':a[m+'_total_ms'],'current_median_ms':x[m+'_total_ms'],'relative_change':x[m+'_total_ms']/a[m+'_total_ms']-1} for m in ['NATIVE','FGAC','INLINE']}
    entry['blocks']=[]
    for b in range(3):
        rs=[r for r in rows if r['case']==c and not r['warmup'] and r['rep']//20==b]
        entry['blocks'].append({'reps':[20*b,20*b+19],'paired_mean_degradation':statistics.mean(ratios[20*b:20*b+20]),'medians_ms':{m:statistics.median(r['total_ms'] for r in rs if r['mode']==m) for m in ['NATIVE','FGAC','INLINE']}})
    comparison.append(entry)
orders=read(p/'formal/orders.json')
from collections import Counter
for c,seq in orders.items():assert len(seq)==60 and set(Counter(tuple(x) for x in seq).values())=={10}
assert len([r for r in rows if r['warmup']])==120
for m in ['NATIVE','FGAC','INLINE']:assert len([r for r in rows if r['warmup'] and r['mode']==m])==40
assert len(read(p/'formal/negative-controls.json'))==2
(p/'comparison.json').write_text(json.dumps(comparison,indent=2))
print(json.dumps(comparison,indent=2))
