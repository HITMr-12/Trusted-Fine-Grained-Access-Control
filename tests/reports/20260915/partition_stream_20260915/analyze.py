import json,statistics,random
from pathlib import Path
p=Path(__file__).resolve().parent
records=[json.loads(x) for x in (p/'e/formal/records.jsonl').read_text().splitlines()]
streams={x['job_id']:x for x in [json.loads(x) for x in (p/'r/remote-stages.jsonl').read_text().splitlines()] if x['kind']=='stream'}
for mode in ['COLLECT','STREAM']:
 xs=[x for x in records if x['mode']==mode];audit=[json.loads(x) for x in (p/'r'/('audit-'+mode+'.jsonl')).read_text().splitlines()]
 assert len(xs)==len(audit)
 for x,a in zip(xs,audit):
  assert a['status']=='complete' and a['rows']==int(x['digest']['rows']) and a['batches']==x['stream_batches']
  st=streams[a['job_id']];x.update(remote_job_id=a['job_id'],r_stream_ms=st['ms'],r_cpu_ms=st['cpu_ms'],probe=st['probe'])
(p/'verified-records.json').write_text(json.dumps(records))
rng=random.Random(91508)
def ci(vals,block=5):
 dist=[];n=len(vals)
 for _ in range(5000):
  draw=[]
  while len(draw)<n:
   k=rng.randrange(n);draw.extend(vals[(k+j)%n] for j in range(block))
  dist.append(statistics.mean(draw[:n]))
 dist.sort();return [dist[125],dist[4875]]
summary=[];pairs=[]
for case in ['bob_90','bob_100']:
 base=sorted([x for x in records if x['case']==case and x['mode']=='COLLECT' and not x['warmup']],key=lambda x:x['rep']);new=sorted([x for x in records if x['case']==case and x['mode']=='STREAM' and not x['warmup']],key=lambda x:x['rep']);assert len(base)==len(new)==60
 vals=[100*(b['total_ms']/a['total_ms']-1) for a,b in zip(base,new)]
 for a,b,v in zip(base,new,vals):pairs.append({'case':case,'rep':a['rep'],'collect_ms':a['total_ms'],'stream_ms':b['total_ms'],'delta_pct':v})
 row={'case':case,'n':60,'paired_mean_delta_pct':statistics.mean(vals),'ci_block5':ci(vals),'ci_block10':ci(vals,10),'block20_means':[statistics.mean(vals[i:i+20]) for i in range(0,60,20)]}
 for mode,xs in [('collect',base),('stream',new)]:
  row[mode]={k:statistics.median(x[k] for x in xs) for k in ['total_ms','first_arrow_ms','e_cpu_ms','r_cpu_ms','r_stream_ms','bridge_next_wait_ms','bridge_ipc_write_ms','stream_batches','arrow_bytes']}
 row['probe']={k:statistics.median(x['probe'][k] for x in new) for k in new[0]['probe']}
 row['first_write_before_first_partition_count']=sum(x['probe']['first_written_ms']<x['probe']['first_partition_done_ms'] for x in new)
 summary.append(row)
(p/'summary.json').write_text(json.dumps(summary,indent=2));(p/'pairs.json').write_text(json.dumps(pairs,indent=2));print(json.dumps(summary,indent=2))
