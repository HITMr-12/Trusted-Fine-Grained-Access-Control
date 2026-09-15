import json,statistics
from pathlib import Path
p=Path(__file__).resolve().parent
rows=json.loads((p/'e/wire-formal.json').read_text());assert len(rows)==30
summary=[]
for case in ['bob_90','bob_100']:
    plain=statistics.median(x['tcp_received_bytes'] for x in rows if x['case']==case and x['mode']=='NONE')
    for mode in ['NONE','LZ4','ZSTD']:
        xs=[x for x in rows if x['case']==case and x['mode']==mode];assert len(xs)==5
        value=statistics.median(x['tcp_received_bytes'] for x in xs)
        summary.append({'case':case,'mode':mode,'n':5,'tcp_received_bytes_median':value,'tcp_received_bytes_min':min(x['tcp_received_bytes'] for x in xs),'tcp_received_bytes_max':max(x['tcp_received_bytes'] for x in xs),'saved_fraction_vs_NONE':1-value/plain,'arrow_logical_bytes':xs[0]['arrow_logical_bytes']})
(p/'wire-summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
