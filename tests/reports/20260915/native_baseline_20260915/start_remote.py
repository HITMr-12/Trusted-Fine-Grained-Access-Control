import json,os,signal,subprocess,time
from pathlib import Path
r=Path('/data1/lyb/fgac-lab');d=r/'deploy/fgac-current';p=r/'manifests/fgac-current.pid'
if p.exists():
    pid=int(p.read_text());proc=Path('/proc')/str(pid)
    if proc.exists():
        assert str(d) in (proc/'cmdline').read_bytes().decode(errors='replace')
        os.kill(pid,signal.SIGTERM)
        for _ in range(100):
            if not proc.exists():break
            time.sleep(.1)
        assert not proc.exists(),'Prior lab Remote did not stop'
env=os.environ.copy();env.update({'SPARK_CONF_DIR':str(d/'conf'),'JAVA_HOME':str(r/'envs/jre11/usr/lib/jvm/java-11-openjdk-arm64'),'PYTHONPATH':str(r/'envs/fgac-python')+':'+str(d),'PYSPARK_PYTHON':'/usr/bin/python3','POLARIS_URL':'http://172.168.22.23:18184','FGAC_ICEBERG_TABLE':'fgac.nyc.taxi_trips','SPARK_LOCAL_DIRS':str(r/'tmp'),'TMPDIR':str(r/'tmp')})
p=subprocess.Popen(['/home/lyb/tools/spark/bin/spark-submit',str(d/'serve_remote.py')],cwd=d,env=env,stdin=subprocess.DEVNULL,stdout=(r/'logs/fgac-current.log').open('a'),stderr=subprocess.STDOUT,start_new_session=True);print('Remote launcher PID',p.pid)
