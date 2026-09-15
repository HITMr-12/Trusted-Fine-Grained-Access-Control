import base64, json, urllib.request, urllib.error
from pathlib import Path
import xml.etree.ElementTree as ET

r = Path('/home/lyb/fgac-lab')
creds = json.loads((r/'deploy/ranger-credentials.json').read_text())
def api(method, path, body=None):
    headers = {'Authorization': 'Basic '+base64.b64encode(('admin:'+creds['admin_password']).encode()).decode(), 'Content-Type':'application/json'}
    req = urllib.request.Request('http://127.0.0.1:16080'+path, data=json.dumps(body).encode() if body is not None else None, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            data = response.read()
            return json.loads(data) if data else None
    except urllib.error.HTTPError as e:
        print('API failed', method, path, e.code, e.read().decode()[:1000]); raise

users = api('GET','/service/xusers/users?pageSize=1000')
usernames = {u['name'] for u in users.get('vXUsers',[])}
for name in ['lyb','alice','bob','denied']:
    if name not in usernames:
        api('POST','/service/xusers/users',{'name':name,'firstName':name,'password':'Lab9_'+__import__('secrets').token_hex(20),'status':1,'isVisible':1,'userSource':1,'userRoleList':['ROLE_USER']})
services = api('GET','/service/public/v2/api/service')
if not any(s['name']=='fgac_spark' for s in services):
    api('POST','/service/public/v2/api/service', {'name':'fgac_spark','type':'hive','isEnabled':True,'configs':{'username':'lyb','password':'unused','jdbc.driverClassName':'org.apache.hive.jdbc.HiveDriver','jdbc.url':'jdbc:hive2://localhost:10000','policy.download.auth.users':'lyb,alice,bob,denied'}})
existing = api('GET','/service/public/v2/api/policy?serviceName=fgac_spark')
names = {p['name'] for p in existing}
resource = {'database':{'values':['baseline','nyc'],'isExcludes':False,'isRecursive':False},'table':{'values':['*'],'isExcludes':False,'isRecursive':False},'column':{'values':['*'],'isExcludes':False,'isRecursive':False}}
allow = {'service':'fgac_spark','name':'lab-select','policyType':0,'isEnabled':True,'isAuditEnabled':True,'resources':resource,'policyItems':[{'users':['lyb','alice','bob'],'accesses':[{'type':'select','isAllowed':True}],'delegateAdmin':False}]}
row = {'service':'fgac_spark','name':'lab-alice-row-filter','policyType':2,'isEnabled':True,'isAuditEnabled':True,'resources':{'database':{'values':['baseline']},'table':{'values':['authz_fixture']}},'rowFilterPolicyItems':[{'users':['alice'],'accesses':[{'type':'select','isAllowed':True}],'rowFilterInfo':{'filterExpr':'VendorID = 1'}}]}
mask = {'service':'fgac_spark','name':'lab-alice-card-mask','policyType':1,'isEnabled':True,'isAuditEnabled':True,'resources':{'database':{'values':['baseline']},'table':{'values':['authz_fixture']},'column':{'values':['card']}},'dataMaskPolicyItems':[{'users':['alice'],'accesses':[{'type':'select','isAllowed':True}],'dataMaskInfo':{'dataMaskType':'CUSTOM','valueExpr':"CASE WHEN {col} IS NULL THEN NULL ELSE 'MASKED' END"}}]}
for policy in [allow,row,mask]:
    if policy['name'] not in names: api('POST','/service/public/v2/api/policy',policy)
conf=r/'deploy/spark-conf'
def xml(name, props):
    root=ET.Element('configuration')
    for key,value in props.items():
        p=ET.SubElement(root,'property');ET.SubElement(p,'name').text=key;ET.SubElement(p,'value').text=str(value)
    ET.indent(root);ET.ElementTree(root).write(conf/name,encoding='utf-8',xml_declaration=True)
xml('ranger-spark-security.xml', {'ranger.plugin.spark.policy.rest.url':'http://127.0.0.1:16080','ranger.plugin.spark.service.name':'fgac_spark','ranger.plugin.spark.policy.cache.dir':str(r/'cache/ranger'),'ranger.plugin.spark.policy.pollIntervalMs':5000,'ranger.plugin.spark.policy.source.impl':'org.apache.ranger.admin.client.RangerAdminRESTClient'})
xml('ranger-spark-audit.xml', {'xasecure.audit.is.enabled':'true','xasecure.audit.destination.log4j':'true','xasecure.audit.destination.solr':'false','xasecure.audit.destination.hdfs':'false'})
print('Configured native Hive service, select, row filter, mask policies and Kyuubi XML')
