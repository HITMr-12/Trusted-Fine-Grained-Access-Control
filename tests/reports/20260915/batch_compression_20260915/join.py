import json,statistics
from pathlib import Path
p=Path(__file__).resolve().parent
rows=[json.loads(x) for x in (p/'e/formal/records.jsonl').read_text().splitlines()]
stages=[json.loads(x) for x in (p/'r/remote-stages.jsonl').read_text().splitlines()];streams={x['job_id']:x for x in stages if x['kind']=='stream'}
for mode in sorted({x['mode'] for x in rows}):
 queries=[x for x in rows if x['mode']==mode];audits=[json.loads(x) for x in (p/'r'/('audit-'+mode+'.jsonl')).read_text().splitlines()][:len(queries)]
 assert len(audits)==len(queries)
 for x,a in zip(queries,audits):
  assert a['status']=='complete' and a['rows']==int(x['digest']['rows']) and a['batches']==x['stream_batches']
  s=streams[a['job_id']];assert int(x['mode'].split('_')[1])==s['batch_rows']
  x.update(remote_job_id=a['job_id'],r_stream_cpu_ms=s['cpu_ms'],r_stream_ms=s['ms'],combined_stream_cpu_ms=x['e_cpu_ms']+s['cpu_ms'])
(p/'verified-records.json').write_text(json.dumps(rows));out=[]
for case in ['bob_90','bob_100']:
 for mode in sorted({x['mode'] for x in rows}):
  xs=[x for x in rows if x['case']==case and x['mode']==mode and not x['warmup']]
  out.append({'case':case,'mode':mode,**{k:statistics.median(x[k] for x in xs) for k in ['r_stream_cpu_ms','r_stream_ms','combined_stream_cpu_ms']}})
(p/'cpu-summary.json').write_text(json.dumps(out,indent=2));print(json.dumps(out,indent=2))
