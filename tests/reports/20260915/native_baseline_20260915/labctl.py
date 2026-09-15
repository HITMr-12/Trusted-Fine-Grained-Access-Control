"""Manage only the isolated fgac-lab services. No system services are modified."""
import argparse, json, os, signal, subprocess, sys, time
from pathlib import Path
r=Path(__file__).resolve().parent.parent
role='R' if str(r).startswith('/data1/') else 'E'
parser=argparse.ArgumentParser();parser.add_argument('action',choices=['status','start','stop']);parser.add_argument('service',nargs='?',default='all');args=parser.parse_args()
services=['polaris-db','minio','catalog','polaris','fgac-benchmark-auth','fgac-current'] if role=='R' else ['ranger-db','ranger-admin']
if args.service!='all':
    if args.service not in services:raise SystemExit('Unknown lab service')
    services=[args.service]
if args.action=='stop':services.reverse()
def pidpath(name):
    if name.endswith('-db'):return r/'deploy'/name/'postmaster.pid'
    return r/'manifests'/(name+'.pid')
def owned_pid(name):
    p=pidpath(name)
    if not p.exists():return None
    pid=int(p.read_text().splitlines()[0]);proc=Path('/proc')/str(pid)/'cmdline'
    if not proc.exists():return None
    if str(r) not in proc.read_bytes().replace(b'\x00',b' ').decode(errors='replace'):
        raise RuntimeError('PID is not owned by this lab: '+str(pid))
    return pid
for name in services:
    pid=owned_pid(name)
    if args.action=='status':print(name,'running '+str(pid) if pid else 'stopped');continue
    if args.action=='stop':
        if pid:
            os.kill(pid,signal.SIGTERM)
            for _ in range(100):
                if not Path('/proc',str(pid)).exists():break
                time.sleep(.1)
            print(name,'stop requested',pid)
        continue
    if pid:print(name,'already running',pid);continue
    env=os.environ.copy()
    if name.endswith('-db'):
        pg=Path('/usr/lib/postgresql/16/bin') if role=='R' else r/'envs/postgres/usr/lib/postgresql/16/bin'
        if role=='E':env['LD_LIBRARY_PATH']=str(pg.parent/'lib')
        subprocess.run([str(pg/'pg_ctl'),'-D',str(r/'deploy'/name),'-l',str(r/'logs'/(name+'.log')),'start'],env=env,check=True);continue
    if name=='polaris':
        subprocess.run([sys.executable,str(r/'deploy/bootstrap_polaris.py')],check=True);continue
    if name=='fgac-current':
        subprocess.run([sys.executable,str(r/'deploy/fgac-current/start_remote.py')],check=True);continue
    if name=='minio':
        c=json.loads((r/'deploy/storage-credentials.json').read_text());env.update({'MINIO_ROOT_USER':c['access_key'],'MINIO_ROOT_PASSWORD':c['secret_key']})
        cmd=[str(r/'deploy/storage-catalog/bin/minio'),'server',str(r/'deploy/storage-catalog/minio-data'),'--address','172.168.22.23:19100','--console-address','127.0.0.1:19101'];cwd=r
    elif name=='fgac-benchmark-auth':
        cwd=r/'deploy/fgac-current';cmd=['java','--add-modules','jdk.httpserver','-cp',str(cwd/'auth-classes'),'org.apache.polaris.fgac.MinimalPolarisFgac']
    elif name=='catalog':
        folder=r/'deploy/storage-catalog/repo/catalog/polaris-minimal'
        # The compatibility endpoint is separate from Polaris metadata management.
        cmd=['java','--add-modules','jdk.httpserver','-cp',str(folder/'classes'),'org.apache.polaris.fgac.MinimalPolarisFgac'];cwd=folder
    else:
        cmd=json.loads((r/'deploy/start-ranger-command.json').read_text());cwd=r/'deploy/ranger-admin'
    p=subprocess.Popen(cmd,cwd=cwd,env=env,stdin=subprocess.DEVNULL,stdout=(r/'logs'/(name+'.log')).open('a'),stderr=subprocess.STDOUT,start_new_session=True)
    pidpath(name).write_text(str(p.pid));print(name,'started',p.pid)
