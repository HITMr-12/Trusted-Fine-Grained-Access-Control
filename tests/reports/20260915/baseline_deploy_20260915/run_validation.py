import os, subprocess, json
from pathlib import Path
r=Path('/home/lyb/fgac-lab');results=[]
env=os.environ.copy();env.update({'SPARK_CONF_DIR':str(r/'deploy/spark-conf'),'JAVA_HOME':'/usr/lib/jvm/java-11-openjdk-arm64','PYSPARK_PYTHON':'/home/lyb/fgac/venv/bin/python','SPARK_LOCAL_DIRS':str(r/'tmp'),'TMPDIR':str(r/'tmp')})
for user in ['alice','bob','denied']:
    env['HADOOP_USER_NAME']=user
    with (r/('logs/authz-'+user+'.log')).open('w') as log:
        p=subprocess.run(['/home/lyb/fgac/spark/spark-3.5.8-bin-hadoop3/bin/spark-submit',str(r/'deploy/verify_authz.py')],env=env,cwd=r,stdout=log,stderr=subprocess.STDOUT,timeout=180)
    result=json.loads((r/('runs/authz-'+user+'.json')).read_text());result['exit_code']=p.returncode;results.append(result);print(user,p.returncode,result['ok'],flush=True)
(r/'runs/authz-all.json').write_text(json.dumps(results,indent=2))
assert all(x['ok'] and x['exit_code']==0 for x in results)
