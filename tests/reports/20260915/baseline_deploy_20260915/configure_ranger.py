import json, os, secrets, shutil, subprocess, urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

r = Path('/home/lyb/fgac-lab')
admin = r/'deploy/ranger-admin'
classes = admin/'ews/webapp/WEB-INF/classes'
conf = classes/'conf'
shutil.copytree(classes/'conf.dist', conf, dirs_exist_ok=True)
credentials = {'admin_password': 'Fgac9_'+secrets.token_hex(16), 'db_password': 'Db9_'+secrets.token_hex(16)}
p = r/'deploy/ranger-credentials.json'
if p.exists(): credentials = json.loads(p.read_text())
else: p.write_text(json.dumps(credentials)); p.chmod(0o600)
lib = admin/'ews/webapp/WEB-INF/lib'
jdbc = lib/'postgresql-42.7.5.jar'
if not jdbc.exists(): urllib.request.urlretrieve('https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar', jdbc)
env = os.environ.copy()
env['LD_LIBRARY_PATH'] = str(r/'envs/postgres/usr/lib/postgresql/16/lib')
psql = str(r/'envs/postgres/usr/lib/postgresql/16/bin/psql')
sql = "CREATE ROLE rangeradmin LOGIN PASSWORD '"+credentials['db_password']+"';\nCREATE DATABASE ranger OWNER rangeradmin;\n"
check = subprocess.run([psql,'-h',str(r/'tmp'),'-p','15432','-U','postgres','-d','postgres','-Atc',"SELECT 1 FROM pg_roles WHERE rolname='rangeradmin'"],env=env,capture_output=True,text=True,check=True)
if not check.stdout.strip(): subprocess.run([psql,'-h',str(r/'tmp'),'-p','15432','-U','postgres','-d','postgres','-v','ON_ERROR_STOP=1'],input=sql,text=True,env=env,check=True)
props = {
 'ranger.service.http.connector.property.address':'127.0.0.1',
 'ranger.jpa.jdbc.driver':'org.postgresql.Driver',
 'ranger.jpa.jdbc.url':'jdbc:postgresql://127.0.0.1:15432/ranger',
 'ranger.jpa.jdbc.user':'rangeradmin','ranger.jpa.jdbc.password':credentials['db_password'],
 'ranger.jpa.jdbc.dialect':'org.eclipse.persistence.platform.database.PostgreSQLPlatform',
 'ranger.credential.provider.path':'', 'ranger.jpa.jdbc.credential.alias':'',
 'ranger.service.host':'127.0.0.1','ranger.service.http.port':'16080',
 'ranger.service.shutdown.port':'16085','ranger.externalurl':'http://127.0.0.1:16080',
 'ranger.audit.source.type':'solr','ranger.audit.solr.urls':'',
 'ranger.authentication.method':'NONE', 'ranger.service.http.enabled':'true',
 'ranger.service.https.attrib.ssl.enabled':'false',
}
tree = ET.parse(conf/'ranger-admin-site.xml'); root = tree.getroot()
for key,value in props.items():
    node = next((x for x in root.findall('property') if x.findtext('name')==key),None)
    if node is None: node=ET.SubElement(root,'property'); ET.SubElement(node,'name').text=key; ET.SubElement(node,'value')
    node.find('value').text=value
tree.write(conf/'ranger-admin-site.xml',encoding='unicode')
(conf/'ranger-admin-site.xml').chmod(0o600)
(conf/'core-site.xml').write_text('<configuration/>')
install = {'DB_FLAVOR':'POSTGRES','SQL_CONNECTOR_JAR':str(jdbc),'db_host':'127.0.0.1:15432','db_name':'ranger','db_user':'rangeradmin','db_password':credentials['db_password'],'rangerAdmin_password':credentials['admin_password'],'rangerTagsync_password':credentials['admin_password'],'rangerUsersync_password':credentials['admin_password'],'keyadmin_password':credentials['admin_password'],'RANGER_ADMIN_LOG_DIR':str(r/'logs/ranger'),'RANGER_PID_DIR_PATH':str(r/'manifests'),'unix_user':'lyb','unix_group':'lyb'}
p=admin/'install.properties'
original=p.read_text(); lines=[]
for line in original.splitlines():
    key=line.split('=',1)[0]
    if key in install: line=key+'='+install.pop(key)
    lines.append(line)
lines += [k+'='+v for k,v in install.items()]
p.write_text('\n'.join(lines)+'\n');p.chmod(0o600)
(r/'logs/ranger').mkdir(exist_ok=True)
print('RANGER_CONFIG_READY')
