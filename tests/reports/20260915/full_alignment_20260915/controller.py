import getpass,json,socket,sys,time,traceback,hashlib
from pathlib import Path
import paramiko
p=Path(__file__).resolve().parent
clients={};timing=[]
def state(**kw):
 kw['time']=time.time();(p/'controller-status.json').write_text(json.dumps(kw,indent=2));print(json.dumps(kw),flush=True)
def command(port,cmd):
 _,out,err=clients[port].exec_command(cmd,timeout=55)
 text=out.read().decode(errors='replace')+err.read().decode(errors='replace');code=out.channel.recv_exit_status()
 if code:raise RuntimeError('Remote command failed '+str(port)+' '+cmd+'\n'+text[-10000:])
 return text
roots={10023:'/data1/lyb/fgac-lab',10025:'/home/lyb/fgac-lab'}
def read_json(port,path):
 with clients[port].open_sftp() as s:
  with s.open(path) as f:return json.loads(f.read())
def pause(seconds,**kw):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  state(**kw,remaining_seconds=round(end-time.monotonic(),1));time.sleep(min(15,max(0,end-time.monotonic())))
try:
 password=getpass.getpass('SSH password: ')
 for port in [10023,10025]:
  sock=socket.socket();sock.settimeout(20);sock.bind(('7.249.249.221',0));sock.connect(('140.210.239.53',port))
  c=paramiko.SSHClient();c.load_host_keys(str(Path.home()/'.ssh/known_hosts'));c.connect('140.210.239.53',port=port,username='lyb',password=password,sock=sock,allow_agent=False,look_for_keys=False);clients[port]=c
 password=None;state(phase='connected')
 for port,root in roots.items():
  command(port,f'test ! -e {root}/deploy/fgac-full-alignment && cp -a {root}/deploy/fgac-alignment-isolated {root}/deploy/fgac-full-alignment && mkdir {root}/runs/full-alignment-0915')
  with clients[port].open_sftp() as sftp:
   for name in ['benchmark.py','worker.py','manage_r.py','serve_compression.py','verify_end.py','export_evidence.py','sample_full.py','cases.json','reference-digests.json']:sftp.put(str(p/name),root+'/deploy/fgac-full-alignment/'+name)
  command(port,f'nohup python3 {root}/deploy/fgac-full-alignment/sample_full.py '+('R' if port==10023 else 'E')+f' > {root}/runs/full-alignment-0915/sampler.log 2>&1 < /dev/null &')
 preflight="""import json,urllib.request,hashlib
from pathlib import Path
r=Path('/home/lyb/fgac-lab');out=r/'runs/full-alignment-0915';result={}
for name,url in [('catalog','http://127.0.0.1:8181/q/health'),('remote','http://127.0.0.1:8002/health'),('minio','http://127.0.0.1:9100/minio/health/live')]:
 with urllib.request.urlopen(url,timeout=10) as f:result[name]=f.status
result['old_pids']={str(pid):Path('/proc',str(pid),'stat').read_text().split()[21] for pid in [80220,837124,838595,4153454]}
(out/'production-before.json').write_text(json.dumps(result))
cfg={}
for name in ['conf','native-conf','inline-conf']:
 f=r/'deploy/fgac-current'/name/'spark-defaults.conf';cfg[name]={'sha256':hashlib.sha256(f.read_bytes()).hexdigest()}
(out/'config-comparison.json').write_text(json.dumps(cfg))
print('Production health and config fingerprints recorded')
"""
 command(10025,"python3 - <<'PY'\n"+preflight+"\nPY")
 for run in range(1,4):
  start=time.time();state(phase='starting_R',run=run)
  command(10023,f'python3 /data1/lyb/fgac-lab/deploy/fgac-full-alignment/manage_r.py start {run}')
  deadline=time.monotonic()+180
  while True:
   try:
    ready=[read_json(10023,f'/data1/lyb/fgac-lab/runs/full-alignment-0915/round-{run}/{mode}/remote-ready.json') for mode in ['COLLECT','STREAM']]
    break
   except FileNotFoundError:
    if time.monotonic()>deadline:raise TimeoutError('R readiness timeout')
    time.sleep(5)
  state(phase='starting_E',run=run,remote_ready=ready)
  command(10025,f'nohup python3 /home/lyb/fgac-lab/deploy/fgac-full-alignment/benchmark.py /home/lyb/fgac-lab/runs/full-alignment-0915/round-{run} > /home/lyb/fgac-lab/runs/full-alignment-0915/round-{run}.log 2>&1 < /dev/null &')
  deadline=time.monotonic()+7200
  while True:
   try:done=read_json(10025,f'/home/lyb/fgac-lab/runs/full-alignment-0915/round-{run}/done.json')
   except FileNotFoundError:done=None
   if done:
    assert done['ok'] and done['formal_records']==1536 and done['records']==2016 and done['cases']==8,done
    break
   if time.monotonic()>deadline:raise TimeoutError('Benchmark timeout')
   try:progress=read_json(10025,f'/home/lyb/fgac-lab/runs/full-alignment-0915/round-{run}/progress.json')
   except FileNotFoundError:progress={'phase':'session_startup'}
   state(phase='benchmark',run=run,progress=progress);time.sleep(15)
  # done.json is emitted before worker shutdown; wait until every recorded process exits.
  workers=read_json(10025,f'/home/lyb/fgac-lab/runs/full-alignment-0915/round-{run}/workers.json')
  check="from pathlib import Path\nids="+repr([w[k] for w in workers for k in ['pid','java']])+"\nprint(all(not Path('/proc',str(pid)).exists() for pid in ids))"
  deadline=time.monotonic()+90
  while command(10025,"python3 - <<'PY'\n"+check+"\nPY").strip()!='True':
   if time.monotonic()>deadline:raise TimeoutError('E worker shutdown timeout')
   time.sleep(3)
  state(phase='cleanup_R',run=run)
  command(10023,f'python3 /data1/lyb/fgac-lab/deploy/fgac-full-alignment/manage_r.py stop {run}')
  entry={'run':run,'start_epoch':start,'all_test_processes_stopped_epoch':time.time(),'done':done};timing.append(entry)
  if run<3:
   wait_start=time.monotonic();entry['wait_start_epoch']=time.time();pause(300,phase='five_minute_gap',after_run=run)
   entry['wait_end_epoch']=time.time();entry['wait_monotonic_seconds']=time.monotonic()-wait_start;assert entry['wait_monotonic_seconds']>=300
  (p/'restart-timing.json').write_text(json.dumps(timing,indent=2))
 for port,root in roots.items():
  role='R' if port==10023 else 'E';state(phase='verify_and_export',role=role)
  command(port,f'python3 {root}/deploy/fgac-full-alignment/verify_end.py {role}')
  # Export runs independently so compression cannot hold the SSH command open.
  command(port,f'nohup sh -c "python3 {root}/deploy/fgac-full-alignment/export_evidence.py {role} && touch {root}/runs/full-alignment-0915/export-done" > {root}/runs/full-alignment-0915/export.log 2>&1 < /dev/null &')
 for port,root in roots.items():
  role='R' if port==10023 else 'E';deadline=time.monotonic()+600
  while True:
   if command(port,f'test -e {root}/runs/full-alignment-0915/export-done && echo READY || true').strip()=='READY':break
   if time.monotonic()>deadline:raise TimeoutError('Evidence export timeout')
   state(phase='exporting',role=role);time.sleep(15)
  state(phase='downloading',role=role)
  with clients[port].open_sftp() as sftp:
   sftp.get(root+'/runs/full-alignment-0915/evidence-'+role+'.tar.gz',str(p/('evidence-'+role+'.tar.gz')))
   sftp.put(str(p/'restart-timing.json'),root+'/runs/full-alignment-0915/restart-timing.json')
 state(phase='complete',ok=True,rounds=3,formal_queries=4608,warmup_queries=1440)
except BaseException:
 (p/'controller-error.txt').write_text(traceback.format_exc(),encoding='utf-8');state(phase='error',error=traceback.format_exc());raise
finally:
 for c in clients.values():c.close()
