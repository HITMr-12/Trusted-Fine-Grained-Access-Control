import json,statistics,collections
from pathlib import Path
p=Path(__file__).resolve().parent
records=[json.loads(x) for x in (p/'e/formal/records.jsonl').read_text().splitlines()];records=[x for x in records if not x['warmup']];lo=min(x['start'] for x in records);hi=max(x['end'] for x in records);output={}
for role in ['R','E']:
 samples=[json.loads(x) for x in (p/role.lower()/('full-resources-'+role+'.jsonl')).read_text().splitlines()];samples=[x for x in samples if lo<=x['time']<=hi]
 busy=[];iow=[];cores=[];top=collections.Counter();names={};swapin=swapout=0;press=[];intervals=[]
 for a,b in zip(samples,samples[1:]):
  dt=b['time']-a['time'];aa=a['stat'].splitlines();bb=b['stat'].splitlines();da=[int(y)-int(x) for x,y in zip(aa[0].split()[1:9],bb[0].split()[1:9])];busy.append(100*(sum(da)-da[3]-da[4])/sum(da));iow.append(100*da[4]/sum(da))
  cpus=[]
  for ac,bc in zip(aa[1:129],bb[1:129]):
   dc=[int(y)-int(x) for x,y in zip(ac.split()[1:9],bc.split()[1:9])];cpus.append(100*(sum(dc)-dc[3]-dc[4])/sum(dc) if sum(dc) else 0)
  cores.append(max(cpus));used={}
  for pid,v in b['processes'].items():
   old=a['processes'].get(pid)
   if old and old['start']==v['start']:
    amount=max(0,v['ticks']-old['ticks'])/100;key=pid+':'+v['start'];top[key]+=amount;names[key]=v['name'];used[key]=amount/dt
  av=dict(x.split() for x in a['vmstat'].splitlines());bv=dict(x.split() for x in b['vmstat'].splitlines());swapin+=int(bv['pswpin'])-int(av['pswpin']);swapout+=int(bv['pswpout'])-int(av['pswpout'])
  intervals.append({'time':b['time'],'busy_pct':busy[-1],'max_core_busy_pct':cores[-1],'top':sorted(used.items(),key=lambda x:x[1],reverse=True)[:5]})
 duration=samples[-1]['time']-samples[0]['time'];output[role]={'samples':len(samples),'cpu_mean_pct':statistics.mean(busy),'cpu_peak_pct':max(busy),'max_single_core_busy_pct':max(cores),'iowait_mean_pct':statistics.mean(iow),'swapin_pages':swapin,'swapout_pages':swapout,'top_processes':[{'pid_start':k,'name':names[k],'mean_cores':v/duration} for k,v in top.most_common(15)],'pressure_last':samples[-1]['pressure']}
 (p/('load-intervals-'+role+'.json')).write_text(json.dumps(intervals))
(p/'load-summary.json').write_text(json.dumps(output,indent=2));print(json.dumps(output,indent=2))
