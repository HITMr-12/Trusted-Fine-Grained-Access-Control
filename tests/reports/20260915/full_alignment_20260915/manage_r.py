import json,os,signal,subprocess,sys,time,socket,hashlib
from pathlib import Path
r=Path('/data1/lyb/fgac-lab');d=r/'deploy/fgac-full-alignment';round_id=int(sys.argv[2]);out=r/'runs/full-alignment-0915'/('round-'+str(round_id))
def live(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[0]!='Z'
 except FileNotFoundError:return False
if sys.argv[1]=='start':
 out.mkdir(parents=True,exist_ok=False)
 for mode,port in [('COLLECT',18844),('STREAM',18845)]:
  with socket.socket() as sock:assert sock.connect_ex(('172.168.22.23',port))!=0,'Port already used'
  run=out/mode;run.mkdir();(run/'events').mkdir();(run/'tmp').mkdir()
  env=os.environ.copy();env.pop('FGAC_ARROW_DELIVERY',None)
  env.update({'SPARK_CONF_DIR':str(r/'deploy/fgac-current/conf'),'JAVA_HOME':str(r/'envs/jre11/usr/lib/jvm/java-11-openjdk-arm64'),'PYTHONPATH':str(r/'envs/fgac-python')+':'+str(d),'PYSPARK_PYTHON':'/usr/bin/python3','POLARIS_URL':'http://172.168.22.23:18184','FGAC_ICEBERG_TABLE':'fgac.nyc.taxi_trips','SPARK_LOCAL_DIRS':str(run/'tmp'),'TMPDIR':str(run/'tmp')})
  jars=next(line.split(None,1)[1].strip() for line in (r/'deploy/fgac-current/conf/spark-defaults.conf').read_text().splitlines() if line.startswith('spark.jars '))+','+str(d/'partition-arrow-stream.jar')
  proc=subprocess.Popen(['/home/lyb/tools/spark/bin/spark-submit','--jars',jars,'--conf','spark.eventLog.dir=file:'+str(run/'events'),str(d/'serve_compression.py'),str(run),mode,str(port)],cwd=d,env=env,stdin=subprocess.DEVNULL,stdout=(run/'server.log').open('w'),stderr=subprocess.STDOUT,start_new_session=True)
  (run/'launcher.json').write_text(json.dumps({'pid':proc.pid,'mode':mode,'port':port}))
 print('Started isolated R launchers',round_id)
else:
 result=[]
 for mode in ['COLLECT','STREAM']:
  run=out/mode;ready=json.loads((run/'remote-ready.json').read_text());j=ready['java'];py=ready['python']
  for pid in [j,py]:assert '/deploy/fgac-full-alignment/' in Path('/proc',str(pid),'cmdline').read_bytes().decode().replace('\0',' ')
  assert os.getpgid(j)==j and os.getpgid(py)==j
  os.killpg(j,signal.SIGTERM);time.sleep(5)
  remaining=[pid for pid in [j,py] if live(pid)]
  for pid in remaining:
   assert '/deploy/fgac-full-alignment/' in Path('/proc',str(pid),'cmdline').read_bytes().decode().replace('\0',' ')
   os.kill(pid,signal.SIGKILL)
  time.sleep(1);assert not any(live(pid) for pid in [j,py]);result.append({'mode':mode,'stopped':True,'forced':remaining})
 (out/'cleanup.json').write_text(json.dumps(result));print(json.dumps(result))
