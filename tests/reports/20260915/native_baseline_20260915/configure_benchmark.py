import json,os,shutil,sys
from pathlib import Path
role=sys.argv[1];r=Path('/data1/lyb/fgac-lab' if role=='R' else '/home/lyb/fgac-lab');d=r/'deploy/fgac-current';out=r/'runs/native-baseline-0915'
storage=json.loads((r/'deploy/storage-credentials.json').read_text());polaris=json.loads((r/'deploy/polaris-credentials.json').read_text())
jars=([str(p) for p in Path('/home/lyb/tools/iceberg-jars').glob('*.jar')]+[str(p) for p in Path('/home/lyb/tools/s3a-jars').glob('*.jar')]) if role=='R' else [str(p) for p in Path('/home/lyb/fgac/jars').glob('*.jar')]
props={'spark.master':'local[2]','spark.driver.memory':'2g','spark.ui.enabled':'false','spark.sql.shuffle.partitions':'200','spark.sql.adaptive.enabled':'true','spark.default.parallelism':'2','spark.sql.execution.arrow.maxRecordsPerBatch':'8192','spark.local.dir':str(r/'tmp'),'spark.jars':','.join(sorted(jars)),'spark.sql.catalog.fgac':'org.apache.iceberg.spark.SparkCatalog','spark.sql.catalog.fgac.type':'rest','spark.sql.catalog.fgac.uri':'http://172.168.22.23:18182/api/catalog','spark.sql.catalog.fgac.warehouse':'fgac','spark.sql.catalog.fgac.credential':polaris['client_id']+':'+polaris['client_secret'],'spark.sql.catalog.fgac.scope':'PRINCIPAL_ROLE:ALL','spark.sql.catalog.fgac.oauth2-server-uri':'http://172.168.22.23:18182/api/catalog/v1/oauth/tokens','spark.sql.catalog.fgac.io-impl':'org.apache.iceberg.hadoop.HadoopFileIO','spark.sql.catalog.fgac.cache-enabled':'true','spark.sql.catalog.fgac.cache.expiration-interval-ms':'30000','spark.hadoop.fs.s3a.endpoint':'http://172.168.22.23:19100','spark.hadoop.fs.s3a.access.key':storage['access_key'],'spark.hadoop.fs.s3a.secret.key':storage['secret_key'],'spark.hadoop.fs.s3a.path.style.access':'true','spark.hadoop.fs.s3a.connection.ssl.enabled':'false','spark.hadoop.fs.s3a.impl':'org.apache.hadoop.fs.s3a.S3AFileSystem','spark.hadoop.fs.s3.impl':'org.apache.hadoop.fs.s3a.S3AFileSystem','spark.eventLog.enabled':'true','spark.eventLog.dir':'file:'+str(out/'events'),'spark.driver.extraJavaOptions':'-XX:ActiveProcessorCount=4','spark.executor.extraJavaOptions':'-XX:ActiveProcessorCount=4'}
props.pop('spark.driver.extraJavaOptions',None);props.pop('spark.executor.extraJavaOptions',None)
(out/'events').mkdir(exist_ok=True)
def write(name,p):
    conf=d/name;conf.mkdir(exist_ok=True)
    f=conf/'spark-defaults.conf';f.write_text('\n'.join(k+' '+v for k,v in p.items())+'\n');f.chmod(0o600)
write('conf',props)
if role=='E':
    props['spark.jars']+=','+str(d/'arrow-socket-source.jar');write('conf',props)
    write('inline-conf',props)
    props['spark.jars']+=','+str(r/'envs/kyuubi-spark-authz-shaded_2.12-1.10.2.jar')
    props['spark.sql.extensions']='org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension'
    props['spark.driver.extraClassPath']=str(d/'native-conf');props['spark.executor.extraClassPath']=str(d/'native-conf')
    write('native-conf',props)
    for name in ['ranger-spark-security.xml','ranger-spark-audit.xml']:
        text=(r/'deploy/spark-conf'/name).read_text().replace('fgac_spark','fgac_bench_0915').replace('/cache/ranger','/cache/ranger-benchmark')
        (d/'native-conf'/name).write_text(text)
print(role,'benchmark configurations ready')
