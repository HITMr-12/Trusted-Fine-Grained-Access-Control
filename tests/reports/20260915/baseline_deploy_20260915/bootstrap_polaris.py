import json, os, secrets, subprocess
from pathlib import Path
r=Path('/data1/lyb/fgac-lab')
p=r/'deploy/polaris-credentials.json'
if not p.exists():
    p.write_text(json.dumps({'client_id':'fgacroot','client_secret':secrets.token_hex(24),'db_password':secrets.token_hex(24)}));p.chmod(0o600)
c=json.loads(p.read_text())
psql=['/usr/lib/postgresql/16/bin/psql','-h',str(r/'tmp'),'-p','15433','-U','postgres','-d','postgres']
check=subprocess.run(psql+['-Atc',"SELECT 1 FROM pg_roles WHERE rolname='polaris'"],capture_output=True,text=True,check=True)
if not check.stdout.strip():
    subprocess.run(psql+['-v','ON_ERROR_STOP=1'],input="CREATE ROLE polaris LOGIN PASSWORD '"+c['db_password']+"';\nCREATE DATABASE polaris OWNER polaris;",text=True,check=True)
env=os.environ.copy()
env.update({'POLARIS_PERSISTENCE_TYPE':'relational-jdbc','QUARKUS_DATASOURCE_USERNAME':'polaris','QUARKUS_DATASOURCE_PASSWORD':c['db_password'],'QUARKUS_DATASOURCE_JDBC_URL':'jdbc:postgresql://127.0.0.1:15433/polaris','POLARIS_REALM_CONTEXT_REALMS':'POLARIS','QUARKUS_HTTP_HOST':'172.168.22.23','QUARKUS_HTTP_PORT':'18182','QUARKUS_MANAGEMENT_HOST':'127.0.0.1','QUARKUS_MANAGEMENT_PORT':'18183','AWS_REGION':'us-east-1'})
storage=json.loads((r/'deploy/storage-credentials.json').read_text())
env.update({'AWS_ACCESS_KEY_ID':storage['access_key'],'AWS_SECRET_ACCESS_KEY':storage['secret_key']})
java=str(r/'envs/jre21/usr/lib/jvm/java-21-openjdk-arm64/bin/java')
dist=r/'envs/polaris-bin-1.7.0'
if not (r/'manifests/polaris-bootstrapped').exists():
    log=r/'logs/polaris-bootstrap.private.log';log.touch(mode=0o600)
    with log.open('w') as f:
        result=subprocess.run([java,'-jar',str(dist/'admin/quarkus-run.jar'),'bootstrap','-r','POLARIS','-c','POLARIS,'+c['client_id']+','+c['client_secret']],env=env,stdout=f,stderr=subprocess.STDOUT)
    if result.returncode:
        print(log.read_text().replace(c['client_secret'],'[REDACTED]').replace(c['db_password'],'[REDACTED]')[-5000:]);raise SystemExit(result.returncode)
    (r/'manifests/polaris-bootstrapped').touch()
p=subprocess.Popen([java,'-Xms256m','-Xmx1g','-jar',str(dist/'server/quarkus-run.jar')],cwd=dist,env=env,stdin=subprocess.DEVNULL,stdout=(r/'logs/polaris-server.log').open('a'),stderr=subprocess.STDOUT,start_new_session=True)
(r/'manifests/polaris.pid').write_text(str(p.pid));print('Polaris PID',p.pid)
