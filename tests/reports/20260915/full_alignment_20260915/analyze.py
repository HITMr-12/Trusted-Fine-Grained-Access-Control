import json,statistics,random,csv,collections
from pathlib import Path
p=Path(__file__).resolve().parent
allrows=[];pairs=[];summary=[];isolation=[]
comparisons=[('STREAM','COLLECT'),('STREAM','NATIVE'),('COLLECT','NATIVE'),('STREAM','INLINE'),('COLLECT','INLINE'),('NATIVE','INLINE')]
fields=['total_ms','prepare_ms','digest_setup_ms','action_ms','e_cpu_ms','first_arrow_ms','bridge_next_wait_ms','bridge_ipc_write_ms','r_stream_ms','r_cpu_ms','arrow_bytes','stream_batches']
rng=random.Random(91800)
def ci(vals,block=4):
 dist=[];n=len(vals)
 for _ in range(3000):
  draw=[]
  while len(draw)<n:
   k=rng.randrange(n);draw.extend(vals[(k+j)%n] for j in range(block))
  dist.append(statistics.mean(draw[:n]))
 dist.sort();return [dist[75],dist[2925]]
for run in range(1,4):
 e=p/'e'/('round-'+str(run));r=p/'r'/('round-'+str(run))
 if not (e/'done.json').exists():continue
 done=json.loads((e/'done.json').read_text());assert done['ok'] and done['formal_records']==1536,done
 assert json.loads((e/'ranger-policies-before.json').read_text())==json.loads((e/'ranger-policies-after.json').read_text())
 rows=[json.loads(x) for x in (e/'records.jsonl').read_text().splitlines()];assert len(rows)==2016
 ordered=sorted(rows,key=lambda x:x['start'])
 assert all(a['end']<=b['start'] for a,b in zip(ordered,ordered[1:])), 'Overlapping measured queries'
 readies=json.loads((e/'workers.json').read_text());ids=[w[k] for w in readies for k in ['pid','java']];assert len(ids)==len(set(ids))
 rids=[]
 for mode in ['COLLECT','STREAM']:
  ready=json.loads((r/mode/'remote-ready.json').read_text());rids.extend([ready['python'],ready['java']])
  xs=[x for x in rows if x['mode']==mode]
  audits=[json.loads(x) for x in (r/mode/('audit-'+mode+'.jsonl')).read_text().splitlines()]
  assert len(xs)==len(audits)==464,(run,mode,len(xs),len(audits))
  stages=[json.loads(x) for x in (r/mode/'remote-stages.jsonl').read_text().splitlines()]
  streams={x['job_id']:x for x in stages if x['kind']=='stream'}
  for x,a in zip(xs,audits):
   assert a['status']=='complete' and a['rows']==int(x['digest']['rows']) and a['batches']==x['stream_batches']
   st=streams[a['job_id']];assert st['delivery']==mode
   x.update(remote_job_id=a['job_id'],r_stream_ms=st['ms'],r_cpu_ms=st['cpu_ms'],probe=st['probe'])
 assert len(rids)==len(set(rids))
 isolation.append({'run':run,'distinct_e_pids':ids,'distinct_r_pids':rids,'no_measured_query_overlap':True,'policy_unchanged':True})
 for x in rows:x['run']=run
 allrows+=rows
 formal=[x for x in rows if not x['warmup']]
 for case in [c['name'] for c in json.loads((p/'cases.json').read_text())]:
  groups={mode:sorted([x for x in formal if x['case']==case and x['mode']==mode],key=lambda x:x['rep']) for mode in ['COLLECT','STREAM','NATIVE','INLINE']}
  assert all(len(xs)==48 for xs in groups.values())
  for i in range(48):
   xs=[g[i] for g in groups.values()];assert all(x['digest']==xs[0]['digest'] and x['schema']==xs[0]['schema'] for x in xs)
   pair={'run':run,'case':case,'rep':i,**{m:groups[m][i]['total_ms'] for m in groups}}
   pair.update({a+'_vs_'+b:100*(pair[a]/pair[b]-1) for a,b in comparisons});pairs.append(pair)
  for a,b in comparisons:
   xs=[x[a+'_vs_'+b] for x in pairs if x['run']==run and x['case']==case]
   summary.append({'run':run,'case':case,'comparison':a+'_vs_'+b,'paired_mean_pct':statistics.mean(xs),'ci_block4':ci(xs),'ci_block8':ci(xs,8),'block16_means':[statistics.mean(xs[i:i+16]) for i in range(0,48,16)]})
metrics=[]
for run in sorted({x['run'] for x in allrows}):
 for case in [c['name'] for c in json.loads((p/'cases.json').read_text())]:
  for mode in ['COLLECT','STREAM','NATIVE','INLINE']:
   xs=[x for x in allrows if x['run']==run and x['case']==case and x['mode']==mode and not x['warmup']]
   metrics.append({'run':run,'case':case,'mode':mode,**{k:statistics.median(x[k] for x in xs) for k in fields if k in xs[0]}})
for name,value in [('verified-records',allrows),('pairs',pairs),('summary',summary),('metrics',metrics),('isolation-validation',isolation)]:
 (p/(name+'.json')).write_text(json.dumps(value,indent=2))
 if name in ['pairs','summary'] and value:
  with (p/(name+'.csv')).open('w',newline='',encoding='utf-8-sig') as f:
   w=csv.DictWriter(f,fieldnames=list(value[0]));w.writeheader();w.writerows(value)
print('Verified',len(allrows),'records;',len(pairs),'four-way groups')
