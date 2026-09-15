import json,statistics
from pathlib import Path
p=Path(__file__).resolve().parent
rows=json.loads((p/'verified-records.json').read_text());summary=json.loads((p/'summary.json').read_text());metrics=json.loads((p/'metrics.json').read_text());out=[]
for case in [c['name'] for c in json.loads((p/'cases.json').read_text())]:
 for comp in sorted({x['comparison'] for x in summary}):
  xs=[x for x in summary if x['case']==case and x['comparison']==comp]
  out.append({'case':case,'comparison':comp,'restart_equal_mean_pct':statistics.mean(x['paired_mean_pct'] for x in xs),'restart_mean_range':[min(x['paired_mean_pct'] for x in xs),max(x['paired_mean_pct'] for x in xs)],'restart_count':len(xs)})
probes=[]
for run in range(1,4):
 for case in [c['name'] for c in json.loads((p/'cases.json').read_text())]:
  xs=[x for x in rows if x['run']==run and x['case']==case and x['mode']=='STREAM' and not x['warmup']]
  probes.append({'run':run,'case':case,'n':len(xs),'batches':statistics.median(x['stream_batches'] for x in xs),'arrow_bytes':statistics.median(x['arrow_bytes'] for x in xs),'write_before_partition_done':sum(x['probe']['first_written_ms']<x['probe']['first_partition_done_ms'] for x in xs),'first_partition_lead_ms':statistics.median(x['probe']['first_partition_done_ms']-x['probe']['first_written_ms'] for x in xs),'queue_wait_ms':statistics.median(x['probe']['producer_queue_ms'] for x in xs),'partitions':statistics.median(x['probe']['partitions'] for x in xs)})
(p/'overall.json').write_text(json.dumps(out,indent=2));(p/'probe-summary.json').write_text(json.dumps(probes,indent=2))
print('Aggregated',len(out),'comparisons')
