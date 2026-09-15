import json,statistics,random
from pathlib import Path
p=Path(__file__).resolve().parent
rows=json.loads((p/'verified-records.json').read_text());pairs=json.loads((p/'pairs.json').read_text());cases=[x['name'] for x in json.loads((p/'cases.json').read_text())]
groups={'all8':cases,'high2':['bob_90','bob_100'],'small2':['bob_001','bob_01'],'whole_source':['whole_source_100']};out=[]
for run in range(1,4):
 for label,names in groups.items():
  for a,b in [('STREAM','NATIVE'),('COLLECT','NATIVE'),('STREAM','COLLECT'),('STREAM','INLINE')]:
   xs=[x for x in pairs if x['run']==run and x['case'] in names]
   vals=[statistics.mean(x[a+'_vs_'+b] for x in xs if x['rep']==i) for i in range(48)]
   out.append({'run':run,'group':label,'comparison':a+'_vs_'+b,'equal_case_mean_pct':statistics.mean(vals),'sum_time_ratio_pct':100*(sum(x[a] for x in xs)/sum(x[b] for x in xs)-1),'block16_means':[statistics.mean(vals[i:i+16]) for i in [0,16,32]]})
aggregate=[]
for label in groups:
 for comp in ['STREAM_vs_NATIVE','COLLECT_vs_NATIVE','STREAM_vs_COLLECT','STREAM_vs_INLINE']:
  xs=[x for x in out if x['group']==label and x['comparison']==comp]
  aggregate.append({'group':label,'comparison':comp,'three_restart_mean_pct':statistics.mean(x['equal_case_mean_pct'] for x in xs),'restart_values':[x['equal_case_mean_pct'] for x in xs]})
(p/'group-summary.json').write_text(json.dumps(out,indent=2));(p/'group-overall.json').write_text(json.dumps(aggregate,indent=2))
# Protocol and exact inter-restart waits are checked independently of performance values.
timing=json.loads((p/'restart-timing.json').read_text());assert len(timing)==3
for a,b in zip(timing,timing[1:]):
 assert a['wait_monotonic_seconds']>=300 and b['start_epoch']>=a['wait_end_epoch']
for run in range(1,4):
 from collections import Counter
 e=p/'e'/('round-'+str(run));orders=json.loads((e/'orders.json').read_text());assert len(orders)==8
 assert all(len(Counter(tuple(o) for o in v))==24 and set(Counter(tuple(o) for o in v).values())=={2} for v in orders.values())
 assert all(not x['ok'] for x in json.loads((e/'negative-controls.json').read_text()))
 for f in e.glob('snapshot-*.json'):assert json.loads(f.read_text())['snapshot']==6896028106384817739
 x=[q for q in rows if q['run']==run and q['warmup']];counts=Counter((q['mode'],q['principal'] if q['mode']=='NATIVE' else 'lyb') for q in x);assert len(counts)==6 and set(counts.values())=={80}
(p/'protocol-validation.json').write_text(json.dumps({'ok':True,'three_independent_restarts':True,'formal_queries':4608,'warmup_queries':1440,'24_orders_each_twice_per_case_per_run':True,'negative_controls_passed':True,'snapshot_fixed':True,'both_gaps_at_least_300_seconds':True,'timing':timing},indent=2))
print(json.dumps([x for x in aggregate if x['comparison']=='STREAM_vs_NATIVE'],indent=2))
