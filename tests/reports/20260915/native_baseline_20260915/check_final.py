"""Read-only post-benchmark health and policy checks; no service mutations."""
import json,sys,urllib.request
from pathlib import Path
role=sys.argv[1]
r=Path('/data1/lyb/fgac-lab' if role=='R' else '/home/lyb/fgac-lab')
out=r/'runs/native-baseline-0915'
result={'role':role}
if role=='E':
    scope={}
    exec((r/'deploy/configure_authz.py').read_text().split('users =')[0],scope)
    before=json.loads((out/'ranger-policies.json').read_text())
    after=scope['api']('GET','/service/public/v2/api/policy?serviceName=fgac_bench_0915')
    result['policy_unchanged']=before==after
    assert result['policy_unchanged']
    for name,url in [('old_catalog','http://127.0.0.1:8181/q/health'),('old_remote','http://127.0.0.1:8002/health'),('old_minio','http://127.0.0.1:9100/minio/health/live')]:
        with urllib.request.urlopen(url,timeout=10) as f:result[name]=f.status
    result['old_pids']={str(pid):Path('/proc',str(pid),'stat').read_text().split()[21] for pid in [80220,837124,838595,4153454]}
else:
    result['remote_ready']=json.loads((out/'remote-ready.json').read_text())
    for role_name in ['python','java']:
        pid=result['remote_ready'][role_name]
        assert Path('/proc',str(pid)).exists()
    result['minio_top_level']=sorted(p.name for p in (r/'deploy/storage-catalog/minio-data/fgac').iterdir())
    result['no_result_prefix']='results' not in result['minio_top_level']
    assert result['no_result_prefix']
(out/'final-health.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result))
