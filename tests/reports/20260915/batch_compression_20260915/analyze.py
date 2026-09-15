import json,statistics,random,math
from pathlib import Path
p=Path(__file__).resolve().parent
records=[json.loads(x) for x in (p/'e/formal/records.jsonl').read_text().splitlines()];r=[x for x in records if not x['warmup']]
assert len(r)==648
assert all(x['ok'] for x in records)
rng=random.Random(91506)
def ci(vals,block=3):
 n=len(vals);dist=[]
 for _ in range(4000):
  draw=[]
  while len(draw)<n:
   start=rng.randrange(n);draw.extend(vals[(start+j)%n] for j in range(block))
  dist.append(statistics.mean(draw[:n]))
 dist.sort();return [dist[100],dist[3900]]
summary=[];comparisons=[]
for case in ['bob_90','bob_100']:
 groups={mode:sorted([x for x in r if x['case']==case and x['mode']==mode],key=lambda x:x['rep']) for mode in sorted({x['mode'] for x in r})}
 for mode,x in groups.items():
  codec,size=mode.split('_');base=groups['NONE_'+size];orig=groups[codec+'_8192']
  a=[100*(v['total_ms']/b['total_ms']-1) for v,b in zip(x,base)];b=[100*(v['total_ms']/o['total_ms']-1) for v,o in zip(x,orig)]
  row={'case':case,'mode':mode,'n':len(x),'total_ms':statistics.median(v['total_ms'] for v in x),'first_arrow_ms':statistics.median(v['first_arrow_ms'] for v in x),'e_cpu_ms':statistics.median(v['e_cpu_ms'] for v in x),'batches':sorted({v['stream_batches'] for v in x}),'vs_same_size_none_pct':statistics.mean(a),'vs_same_size_none_ci':ci(a),'vs_same_codec_8192_pct':statistics.mean(b),'vs_same_codec_8192_ci':ci(b),'vs_same_codec_8192_ci_block9':ci(b,9),'ipc_write_ms':statistics.median(v['bridge_ipc_write_ms'] for v in x),'queue_wait_ms':statistics.median(v['bridge_next_wait_ms'] for v in x)}
  summary.append(row)
 for codec in ['LZ4','ZSTD']:
  for size in [32768,65536]:
   vals=[]
   for i in range(36):
    extra_big=groups[f'{codec}_{size}'][i]['total_ms']-groups[f'NONE_{size}'][i]['total_ms'];extra_small=groups[codec+'_8192'][i]['total_ms']-groups['NONE_8192'][i]['total_ms'];vals.append(extra_big-extra_small)
   comparisons.append({'case':case,'codec':codec,'size':size,'change_in_compression_extra_ms':statistics.mean(vals),'ci':ci(vals),'ci_block9':ci(vals,9),'block9_means':[statistics.mean(vals[i:i+9]) for i in range(0,36,9)]})
(p/'summary.json').write_text(json.dumps(summary,indent=2));(p/'interaction.json').write_text(json.dumps(comparisons,indent=2));print(json.dumps(summary,indent=2));print(json.dumps(comparisons,indent=2))
