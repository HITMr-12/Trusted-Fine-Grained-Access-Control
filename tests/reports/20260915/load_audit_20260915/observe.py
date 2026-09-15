import os,time,json
from pathlib import Path
hz=os.sysconf('SC_CLK_TCK')
def snap():
 p={}
 for d in Path('/proc').iterdir():
  if not d.name.isdigit():continue
  try:
   raw=(d/'stat').read_text(); v=raw.rsplit(')',1)[1].split();p[d.name]={'name':raw.split('(',1)[1].rsplit(')',1)[0],'ticks':int(v[11])+int(v[12]),'start':v[19],'rss_mib':int(v[21])*os.sysconf('SC_PAGE_SIZE')/1048576}
  except (OSError,ValueError):pass
 return {'time':time.time(),'p':p,'cpu':list(map(int,Path('/proc/stat').read_text().splitlines()[0].split()[1:9])),'load':Path('/proc/loadavg').read_text().strip(),'pressure':{k:Path('/proc/pressure/'+k).read_text() for k in ['cpu','memory','io']},'mem':Path('/proc/meminfo').read_text()}
a=snap();time.sleep(30);b=snap();dt=b['time']-a['time'];top=[]
for pid,v in b['p'].items():
 old=a['p'].get(pid)
 if old and old['start']==v['start']:top.append({'pid':pid,'name':v['name'],'cpu_cores':(v['ticks']-old['ticks'])/hz/dt,'rss_mib':v['rss_mib']})
d=[y-x for x,y in zip(a['cpu'],b['cpu'])]
print(json.dumps({'start':a['time'],'end':b['time'],'host_cpu_busy_pct':100*(sum(d)-d[3]-d[4])/sum(d),'iowait_pct':100*d[4]/sum(d),'load':b['load'],'pressure':b['pressure'],'mem':b['mem'],'top':sorted(top,key=lambda x:x['cpu_cores'],reverse=True)[:25]},indent=2))
