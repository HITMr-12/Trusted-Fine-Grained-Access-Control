import json,statistics,collections
from pathlib import Path
p=Path(__file__).resolve().parent;rows=json.loads((p/'verified-records.json').read_text());result=[]
for role in ['R','E']:
 samples=[]
 with (p/role.lower()/('full-resources-'+role+'.jsonl')).open() as f:
  for line in f:samples.append(json.loads(line))
 for run in sorted({x['run'] for x in rows}):
  xs=[x for x in rows if x['run']==run and not x['warmup']];lo=min(x['start'] for x in xs);hi=max(x['end'] for x in xs)
  ss=[x for x in samples if lo<=x['time']<=hi];busy=[];iow=[];top=collections.Counter();names={};swapi=swapo=0
  for a,b in zip(ss,ss[1:]):
   da=[int(y)-int(x) for x,y in zip(a['stat'].splitlines()[0].split()[1:9],b['stat'].splitlines()[0].split()[1:9])]
   busy.append(100*(sum(da)-da[3]-da[4])/sum(da));iow.append(100*da[4]/sum(da))
   for pid,v in b['processes'].items():
    old=a['processes'].get(pid)
    if old and old['start']==v['start']:
     key=pid+':'+v['start'];top[key]+=max(0,v['ticks']-old['ticks'])/100;names[key]=v['name']
   av=dict(x.split() for x in a['vmstat'].splitlines());bv=dict(x.split() for x in b['vmstat'].splitlines());swapi+=int(bv['pswpin'])-int(av['pswpin']);swapo+=int(bv['pswpout'])-int(av['pswpout'])
  dur=ss[-1]['time']-ss[0]['time']
  result.append({'role':role,'run':run,'cpu_mean_pct':statistics.mean(busy),'cpu_peak_pct':max(busy),'iowait_mean_pct':statistics.mean(iow),'swapin_pages':swapi,'swapout_pages':swapo,'top_processes':[{'pid_start':k,'name':names[k],'mean_cores':v/dur} for k,v in top.most_common(15)]})
(p/'load-summary.json').write_text(json.dumps(result,indent=2));print(json.dumps([{k:v for k,v in x.items() if k!='top_processes'} for x in result]))
