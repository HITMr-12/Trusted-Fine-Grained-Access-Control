import json,os,signal,subprocess,time
from pathlib import Path
r=Path('/data1/lyb/fgac-lab');d=r/'deploy/fgac-partition-stream';p=r/'manifests/fgac-partition-stream.pid'
if p.exists() and Path('/proc',p.read_text().strip()).exists():
    raise SystemExit('Compression service already running; use the existing endpoints')
env=os.environ.copy();env.update({'SPARK_CONF_DIR':str(r/'deploy/fgac-current/conf'),'JAVA_HOME':str(r/'envs/jre11/usr/lib/jvm/java-11-openjdk-arm64'),'PYTHONPATH':str(r/'envs/fgac-python')+':'+str(d),'PYSPARK_PYTHON':'/usr/bin/python3','POLARIS_URL':'http://172.168.22.23:18184','FGAC_ICEBERG_TABLE':'fgac.nyc.taxi_trips','SPARK_LOCAL_DIRS':str(r/'tmp'),'TMPDIR':str(r/'tmp')})
jars=next(line.split(None,1)[1].strip() for line in (r/'deploy/fgac-current/conf/spark-defaults.conf').read_text().splitlines() if line.startswith('spark.jars '))+','+str(d/'partition-arrow-stream.jar')
p=subprocess.Popen(['/home/lyb/tools/spark/bin/spark-submit','--jars',jars,'--conf','spark.eventLog.dir=file:/data1/lyb/fgac-lab/runs/partition-stream-0915/events',str(d/'serve_compression.py')],cwd=d,env=env,stdin=subprocess.DEVNULL,stdout=(r/'logs/fgac-partition-stream.log').open('a'),stderr=subprocess.STDOUT,start_new_session=True);print('Remote launcher PID',p.pid)
