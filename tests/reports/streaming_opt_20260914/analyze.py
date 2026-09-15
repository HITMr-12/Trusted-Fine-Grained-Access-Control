"""Prespecified paired analysis, equal weight to seven scenes and thirty reps."""
import json,csv,statistics,random,sys
from pathlib import Path
p=Path(__file__).resolve().parent;d=p/(sys.argv[1] if len(sys.argv)>1 else 'round1')
def read(f):return json.loads((d/f).read_text())
rows=read('records.json');v={x['id']:x for x in read('validation.json')};cases=read('cases.json')
assert read('done.json')['ok'] and len(rows)==len(v)==735
assert read('done.json')['cleaned']==0
audits=[json.loads(x) for x in (p/'r/audit.jsonl').read_text().splitlines()]
start=read('audit_offset.json')['records'];audits=audits[start:start+490]
streams=[x for x in rows if x['mode']!='A'];assert len(streams)==len(audits)==490
stage=[json.loads(x) for x in (p/'r/stages.jsonl').read_text().splitlines()]
byjob={x['job_id']:x for x in stage if x['kind']=='stream'}
for x,au in zip(streams,audits):
 assert au['status']=='complete' and au['principal']==x['principal'] and au['policy_version']=='2'
 assert au['rows']==x['stream_rows'] and au['batches']==x['stream_batches']
 st=byjob[au['job_id']];assert st['plan_hash']==x['plan_hash']
 x.update(policy_version='2',resolved_principal=au['principal'],server_audit=au,server_stream=st)
 if x['mode']=='O':assert x['prefetch_peak_batches']<=2 and x['prefetch_peak_bytes']<=16*1024*1024
for c in cases:
 for rep in range(-5,30):
  xs=[x for x in rows if x['case']==c['name'] and x['rep']==rep];assert {x['mode'] for x in xs}=={'A','U','O'}
  assert all(v[x['id']]['digest']==v[xs[0]['id']]['digest'] and v[x['id']]['schema']==v[xs[0]['id']]['schema'] for x in xs)
  assert all(x['resolved_principal']==c['principal'] and x['policy_version']=='2' for x in xs)
  assert all(int(v[x['id']]['digest']['rows'])==c['rows'] for x in xs)
(d/'verified_records.json').write_text(json.dumps(rows,indent=2))
def q(xs,f):
 xs=sorted(xs);i=(len(xs)-1)*f;j=int(i);return xs[j]+(xs[min(j+1,len(xs)-1)]-xs[j])*(i-j)
def ci(xs,func=statistics.mean):
 rng=random.Random(914);bs=[func(rng.choices(xs,k=len(xs))) for _ in range(10000)];return [q(bs,.025),q(bs,.975)]
summary=[];pairs=[];comparisons=[]
for c in cases:
 name=c['name']
 for m in ['A','U','O']:
  xs=[x for x in rows if x['case']==name and x['mode']==m and not x['warmup']]
  z={'case':name,'mode':m,'n':len(xs)}
  for k in xs[0]:
   if k.endswith('_ms') or k in ['prefetch_peak_batches','prefetch_peak_bytes','stream_rows','stream_batches','arrow_bytes']:
    z[k]=statistics.median(x[k] for x in xs)
  z.update(q1_ms=q([x['total_ms'] for x in xs],.25),q3_ms=q([x['total_ms'] for x in xs],.75));summary.append(z)
 for rep in range(30):
  xs={x['mode']:x for x in rows if x['case']==name and x['rep']==rep};t={m:x['total_ms'] for m,x in xs.items()}
  pairs.append({'case':name,'rep':rep,**{m+'_ms':t[m] for m in t},'U_over_A':t['U']/t['A'],'O_over_A':t['O']/t['A'],'O_over_U':t['O']/t['U'],'O_minus_U_ms':t['O']-t['U'],'O_minus_A_ms':t['O']-t['A'],'first_batch_delta_ms':xs['O']['first_arrow_ms']-xs['U']['first_arrow_ms']})
 xs=[x for x in pairs if x['case']==name];z={'case':name}
 for k in ['U_over_A','O_over_A','O_over_U','O_minus_U_ms','O_minus_A_ms','first_batch_delta_ms']:
  vals=[x[k] for x in xs];z[k]={'mean':statistics.mean(vals),'median':statistics.median(vals),'mean_ci95':ci(vals)}
 comparisons.append(z)
# Stratified bootstrap keeps all seven scenes equally represented.
rng=random.Random(914);strata=[[x['O_over_A']-1 for x in pairs if x['case']==c['name']] for c in cases]
bs=[statistics.mean(statistics.mean(rng.choices(xs,k=len(xs))) for xs in strata) for _ in range(10000)]
overall={'round':d.name,'formal_queries':630,'pairs':210,'primary':'mean paired ratio O/A minus one; seven scenes equally weighted','U_over_A_mean_degradation':statistics.mean(x['U_over_A']-1 for x in pairs),'O_over_A_mean_degradation':statistics.mean(x['O_over_A']-1 for x in pairs),'O_over_A_degradation_ci95':[q(bs,.025),q(bs,.975)],'O_over_U_mean_ratio':statistics.mean(x['O_over_U'] for x in pairs),'mean_saved_ms':-statistics.mean(x['O_minus_U_ms'] for x in pairs),'total_time_degradation':sum(x['O_ms'] for x in pairs)/sum(x['A_ms'] for x in pairs)-1}
overall['target_met']=overall['O_over_A_mean_degradation']<.10
overall['upper_ci_below_target']=overall['O_over_A_degradation_ci95'][1]<.10
overall['blocks']=[{'block':i,'optimized_degradation':statistics.mean(x['O_over_A']-1 for x in pairs if x['rep']//10==i),'unoptimized_degradation':statistics.mean(x['U_over_A']-1 for x in pairs if x['rep']//10==i)} for i in range(3)]
for name,a in [('summary',summary),('pairs',pairs),('comparisons',comparisons)]:
 (d/(name+'.json')).write_text(json.dumps(a,indent=2))
 if name!='comparisons':
  with (d/(name+'.csv')).open('w',newline='',encoding='utf-8-sig') as f:
   w=csv.DictWriter(f,fieldnames=list(dict.fromkeys(k for x in a for k in x)));w.writeheader();w.writerows(a)
(d/'overall.json').write_text(json.dumps(overall,indent=2));print(json.dumps(overall,indent=2))
