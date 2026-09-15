import json,os,signal,time,urllib.request,sys
from pathlib import Path
role=sys.argv[1];r=Path('/data1/lyb/fgac-lab' if role=='R' else '/home/lyb/fgac-lab');out=r/'runs/final-baseline-0915';result={'role':role}
(out/'stop-sampler').touch()
if role=='R':
 ready=json.loads((out/'remote-ready.json').read_text());j=ready['java'];py=ready['python']
 assert '/deploy/fgac-final-baseline/' in Path('/proc',str(j),'cmdline').read_bytes().decode().replace('\0',' ')
 assert '/deploy/fgac-final-baseline/' in Path('/proc',str(py),'cmdline').read_bytes().decode().replace('\0',' ')
 assert os.getpgid(j)==j and os.getpgid(py)==j
 os.killpg(j,signal.SIGTERM)
 time.sleep(8)
 def live(pid):
  try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[0]!='Z'
  except FileNotFoundError:return False
 remaining=[pid for pid in [j,py] if live(pid)];result['needed_kill_after_term']=remaining
 for pid in remaining:
  assert '/deploy/fgac-final-baseline/' in Path('/proc',str(pid),'cmdline').read_bytes().decode().replace('\0',' ')
  os.kill(pid,signal.SIGKILL)
 time.sleep(1);result['isolated_processes_stopped']=not any(live(pid) for pid in [j,py]);assert result['isolated_processes_stopped']
 result['original_fgac_pid']=int((r/'manifests/fgac-current.pid').read_text());assert result['original_fgac_pid']==2096673
 result['original_fgac_alive']=all(live(pid) for pid in [2096673,2096447]);assert result['original_fgac_alive']
 result['minio_top_level']=sorted(x.name for x in (r/'deploy/storage-catalog/minio-data/fgac').iterdir())
else:
 for name,url in [('old_catalog','http://127.0.0.1:8181/q/health'),('old_remote','http://127.0.0.1:8002/health'),('old_minio','http://127.0.0.1:9100/minio/health/live')]:
  with urllib.request.urlopen(url,timeout=10) as f:result[name]=f.status
 result['old_pids']={str(pid):Path('/proc',str(pid),'stat').read_text().split()[21] for pid in [80220,837124,838595,4153454]}
 workers=json.loads((out/'formal/workers.json').read_text());result['test_workers_stopped']=all(not Path('/proc',str(w[k])).exists() for w in workers for k in ['pid','java']);assert result['test_workers_stopped']
(out/'completion-state.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
