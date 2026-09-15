import os,time,json,sys
from pathlib import Path
role=sys.argv[1];root=Path('/data1/lyb/fgac-lab' if role=='R' else '/home/lyb/fgac-lab');out=root/'runs/full-alignment-0915';hz=os.sysconf('SC_CLK_TCK')
def snap():
 p={}
 for d in Path('/proc').iterdir():
  if not d.name.isdigit():continue
  try:
   raw=(d/'stat').read_text();v=raw.rsplit(')',1)[1].split();p[d.name]={'name':raw.split('(',1)[1].rsplit(')',1)[0],'ticks':int(v[11])+int(v[12]),'start':v[19],'rss':int(v[21])*os.sysconf('SC_PAGE_SIZE')}
  except (OSError,ValueError):pass
 return {'time':time.time(),'processes':p,'stat':Path('/proc/stat').read_text(),'pressure':{k:Path('/proc/pressure/'+k).read_text() for k in ['cpu','memory','io']},'diskstats':Path('/proc/diskstats').read_text(),'net':Path('/proc/net/dev').read_text(),'vmstat':Path('/proc/vmstat').read_text(),'load':Path('/proc/loadavg').read_text()}
with (out/('full-resources-'+role+'.jsonl')).open('w') as f:
 for _ in range(14400):
  if (out/'stop-sampler').exists():break
  f.write(json.dumps(snap())+'\n');f.flush();time.sleep(2)
