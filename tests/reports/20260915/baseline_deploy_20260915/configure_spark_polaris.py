import json
from pathlib import Path
r=Path('/home/lyb/fgac-lab');cp=r/'deploy/polaris-credentials.json';cp.chmod(0o600);c=json.loads(cp.read_text())
p=r/'deploy/spark-conf/spark-defaults.conf'
settings={}
for line in p.read_text().splitlines():
    if line.strip() and not line.startswith('#'):
        key,value=line.split(None,1);settings[key]=value
settings.update({'spark.sql.catalog.fgac.type':'rest','spark.sql.catalog.fgac.uri':'http://172.168.22.23:18182/api/catalog','spark.sql.catalog.fgac.warehouse':'fgac','spark.sql.catalog.fgac.credential':c['client_id']+':'+c['client_secret'],'spark.sql.catalog.fgac.scope':'PRINCIPAL_ROLE:ALL','spark.sql.catalog.fgac.oauth2-server-uri':'http://172.168.22.23:18182/api/catalog/v1/oauth/tokens','spark.sql.catalog.fgac.io-impl':'org.apache.iceberg.hadoop.HadoopFileIO'})
settings['spark.hadoop.fs.s3.impl']='org.apache.hadoop.fs.s3a.S3AFileSystem'
p.write_text('\n'.join(k+' '+v for k,v in settings.items())+'\n');p.chmod(0o600)
print('Spark now uses Polaris REST Catalog')
