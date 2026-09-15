import json,sys,socket,urllib.request,hashlib
from pathlib import Path
role=sys.argv[1];r=Path('/data1/lyb/fgac-lab' if role=='R' else '/home/lyb/fgac-lab');out=r/'runs/alignment-isolated-0915';result={'role':role}
(out/'stop-sampler').touch()
if role=='E':
 for name,url in [('catalog','http://127.0.0.1:8181/q/health'),('remote','http://127.0.0.1:8002/health'),('minio','http://127.0.0.1:9100/minio/health/live')]:
  with urllib.request.urlopen(url,timeout=10) as f:result[name]=f.status
 result['old_pids']={str(pid):Path('/proc',str(pid),'stat').read_text().split()[21] for pid in [80220,837124,838595,4153454]}
 before=json.loads((out/'production-before.json').read_text());assert all(result[k]==v for k,v in before.items())
 workers=[w for f in out.glob('round-*/workers.json') for w in json.loads(f.read_text())]
 result['all_test_workers_gone']=all(not Path('/proc',str(w[k])).exists() for w in workers for k in ['pid','java']);assert result['all_test_workers_gone']
 cfg=json.loads((out/'config-comparison.json').read_text())
 result['original_config_unchanged']=all(hashlib.sha256((r/'deploy/fgac-current'/k/'spark-defaults.conf').read_bytes()).hexdigest()==v['sha256'] for k,v in cfg.items());assert result['original_config_unchanged']
else:
 result['original_fgac_alive']=all(Path('/proc',str(pid)).exists() for pid in [2096673,2096447]);assert result['original_fgac_alive']
 result['original_manifest']=int((r/'manifests/fgac-current.pid').read_text());assert result['original_manifest']==2096673
 result['rounds_cleaned']=[json.loads(f.read_text()) for f in out.glob('round-*/cleanup.json')];assert len(result['rounds_cleaned'])==3
 for port in [18840,18841]:
  with socket.socket() as s:assert s.connect_ex(('172.168.22.23',port))!=0
 result['isolated_ports_closed']=True
(out/'completion-state.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
