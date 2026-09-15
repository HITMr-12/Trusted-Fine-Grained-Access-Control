import json,statistics,random,csv
from pathlib import Path
p=Path(__file__).resolve().parent
records=[json.loads(x) for x in (p/'e/formal/records.jsonl').read_text().splitlines()]
formal=[x for x in records if not x['warmup']];assert len(formal)==864
fgac=[x for x in records if x['mode']=='FGAC'];audits=[json.loads(x) for x in (p/'r/audit-STREAM.jsonl').read_text().splitlines()];assert len(fgac)==len(audits)==368
streams={x['job_id']:x for x in [json.loads(x) for x in (p/'r/remote-stages.jsonl').read_text().splitlines()] if x['kind']=='stream'}
for x,a in zip(fgac,audits):
 assert a['status']=='complete' and a['rows']==int(x['digest']['rows']) and a['batches']==x['stream_batches']
 st=streams[a['job_id']];x.update(remote_job_id=a['job_id'],r_stream_ms=st['ms'],r_cpu_ms=st['cpu_ms'],probe=st['probe'])
(p/'verified-records.json').write_text(json.dumps(records))
rng=random.Random(91511)
def ci(vals,block=3):
 dist=[];n=len(vals)
 for _ in range(5000):
  draw=[]
  while len(draw)<n:
   k=rng.randrange(n);draw.extend(vals[(k+j)%n] for j in range(block))
  dist.append(statistics.mean(draw[:n]))
 dist.sort();return [dist[125],dist[4875]]
cases=json.loads((p/'e/formal/cases.json').read_text());summary=[];pairs=[];deltas={}
for case in cases:
 name=case['name'];groups={mode:sorted([x for x in formal if x['case']==name and x['mode']==mode],key=lambda x:x['rep']) for mode in ['NATIVE','FGAC','INLINE']};assert all(len(x)==36 for x in groups.values())
 for i in range(36):
  a,b,c=[groups[m][i] for m in ['NATIVE','FGAC','INLINE']];assert a['digest']==b['digest']==c['digest'] and a['schema']==b['schema']==c['schema'];pairs.append({'case':name,'rep':i,'native_ms':a['total_ms'],'fgac_ms':b['total_ms'],'inline_ms':c['total_ms'],'fgac_vs_native_pct':100*(b['total_ms']/a['total_ms']-1),'fgac_vs_inline_pct':100*(b['total_ms']/c['total_ms']-1)})
 vals=[x['fgac_vs_native_pct'] for x in pairs if x['case']==name];iv=[x['fgac_vs_inline_pct'] for x in pairs if x['case']==name];deltas[name]=vals
 summary.append({'case':name,'rows':case['rows'],'n':36,'native_ms':statistics.median(x['total_ms'] for x in groups['NATIVE']),'fgac_ms':statistics.median(x['total_ms'] for x in groups['FGAC']),'inline_ms':statistics.median(x['total_ms'] for x in groups['INLINE']),'fgac_vs_native_pct':statistics.mean(vals),'ci_block3':ci(vals),'ci_block6':ci(vals,6),'fgac_vs_inline_pct':statistics.mean(iv),'inline_ci_block3':ci(iv),'block12_means':[statistics.mean(vals[i:i+12]) for i in range(0,36,12)],'fgac_first_arrow_ms':statistics.median(x['first_arrow_ms'] for x in groups['FGAC'])})
overall={}
for label,names in [('all8',[c['name'] for c in cases]),('original7',[c['name'] for c in cases if c['name']!='whole_source_100']),('high2',['bob_90','bob_100'])]:
 vals=[statistics.mean(deltas[n][i] for n in names) for i in range(36)];xs=[x for x in pairs if x['case'] in names];overall[label]={'equal_case_mean_pct':statistics.mean(vals),'ci_block3':ci(vals),'sum_time_ratio_pct':100*(sum(x['fgac_ms'] for x in xs)/sum(x['native_ms'] for x in xs)-1)}
for name,rows in [('summary',summary),('pairs',pairs)]:
 (p/(name+'.json')).write_text(json.dumps(rows,indent=2))
 with (p/(name+'.csv')).open('w',newline='',encoding='utf-8-sig') as f:
  w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
(p/'overall.json').write_text(json.dumps(overall,indent=2));print(json.dumps(summary,indent=2));print(json.dumps(overall,indent=2))
