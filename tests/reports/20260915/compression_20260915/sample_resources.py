"""Read-only one-second counters; no changes to system or production services."""
import json,os,sys,time
from pathlib import Path
role=sys.argv[1];r=Path('/data1/lyb/fgac-lab' if role=='R' else '/home/lyb/fgac-lab');out=r/'runs/compression-0915';hz=os.sysconf('SC_CLK_TCK');page=os.sysconf('SC_PAGE_SIZE')
def proc(pid):
    try:
        p=Path('/proc')/str(pid);a=(p/'stat').read_text().rsplit(')',1)[1].split()
        return {'pid':pid,'cpu_s':(int(a[11])+int(a[12]))/hz,'rss_bytes':int(a[21])*page,'io':dict((k,int(v)) for k,v in (x.split(':') for x in (p/'io').read_text().splitlines()))}
    except (OSError,ValueError):return None
with (out/('resources-'+role+'.jsonl')).open('w') as f:
    for _ in range(3600):
        if (out/'stop-resource-sampler').exists():break
        pids=[]
        if role=='R':
            try:pids.extend(json.loads((out/'remote-ready.json').read_text())[k] for k in ['python','java'])
            except (OSError,ValueError):pass
            for name in ['minio','polaris','fgac-benchmark-auth']:
                try:pids.append(int((r/'manifests'/(name+'.pid')).read_text()))
                except (OSError,ValueError):pass
        else:
            try:
                for worker in json.loads((out/'formal/workers.json').read_text()):pids.extend([worker['pid'],worker['java']])
            except (OSError,ValueError):pass
        net={}
        for line in Path('/proc/net/dev').read_text().splitlines()[2:]:
            name,value=line.split(':');v=value.split();net[name.strip()]={'rx':int(v[0]),'tx':int(v[8])}
        f.write(json.dumps({'time':time.time(),'processes':[x for x in (proc(p) for p in set(pids)) if x],'net':net,'load':Path('/proc/loadavg').read_text().strip(),'cpu':Path('/proc/stat').read_text().splitlines()[0]})+'\n');f.flush();time.sleep(1)
