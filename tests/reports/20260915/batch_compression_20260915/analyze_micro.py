import json,statistics
from pathlib import Path
p=Path(__file__).resolve().parent
rows=[json.loads(x) for x in (p/'e/ipc-microbench.jsonl').read_text().splitlines()];rows=[x for x in rows if not x['warmup']];out=[]
for size in [8192,32768,65536]:
 for codec in ['NONE','LZ4','ZSTD']:
  x=[x for x in rows if x['size']==size and x['codec']==codec]
  out.append({'size':size,'codec':codec,'n':len(x),**{key:statistics.median(v[key] for v in x) for key in ['batches','nonempty_input_buffers','ipc_bytes','encode_ms','decode_ms','encode_cpu_ms','decode_cpu_ms']}})
(p/'micro-summary.json').write_text(json.dumps(out,indent=2));print(json.dumps(out,indent=2))
