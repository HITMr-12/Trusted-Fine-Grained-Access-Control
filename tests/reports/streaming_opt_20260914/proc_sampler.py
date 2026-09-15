"""Passive 100ms process counters and machine interface counters."""
import json,os,sys,time
from pathlib import Path
out=Path(sys.argv[1]);role=sys.argv[2];hz=os.sysconf('SC_CLK_TCK');page=os.sysconf('SC_PAGE_SIZE')
def readproc(pid):
 try:
  p=Path('/proc')/str(pid);a=(p/'stat').read_text().rsplit(')',1)[1].split();io={}
  try:io=dict((k,int(v)) for k,v in (x.split(':') for x in (p/'io').read_text().splitlines()))
  except OSError:pass
  return {'pid':pid,'cpu_s':(int(a[11])+int(a[12]))/hz,'rss_bytes':int(a[21])*page,'io':io,'start_ticks':int(a[19])}
 except (OSError,ValueError,IndexError):return None
deadline=time.monotonic()+3600;n=0
with (out/(role+'_proc.jsonl')).open('w') as f:
 while time.monotonic()<deadline and not (out/'stop_sampler').exists():
  pids=[80220,837124,838595,4153454] if role=='r' else []
  for name in ['probe_java_pid','probe_python_pid']:
   try:pids.append(int((out/name).read_text()))
   except (OSError,ValueError):pass
  try:pids.extend(int(v) for v in json.loads((out/'pid.json').read_text()).values())
  except (OSError,ValueError):pass
  net={}
  for line in Path('/proc/net/dev').read_text().splitlines()[2:]:
   name,values=line.split(':');a=values.split();net[name.strip()]={'rx':int(a[0]),'tx':int(a[8])}
  f.write(json.dumps({'time':time.time(),'processes':[x for x in (readproc(p) for p in set(pids)) if x],'net':net})+'\n');f.flush();n+=1;time.sleep(.1)
(out/(role+'_sampler_done.json')).write_text(json.dumps({'samples':n}))
