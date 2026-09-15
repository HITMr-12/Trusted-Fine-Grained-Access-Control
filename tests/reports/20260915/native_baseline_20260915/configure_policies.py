import json,secrets
from pathlib import Path
r=Path('/home/lyb/fgac-lab');scope={}
exec((r/'deploy/configure_authz.py').read_text().split('users =')[0],scope);api=scope['api'];service='fgac_bench_0915'
users={u['name'] for u in api('GET','/service/xusers/users?pageSize=1000')['vXUsers']}
if 'bench_full' not in users:api('POST','/service/xusers/users',{'name':'bench_full','firstName':'bench_full','password':'Lab9_'+secrets.token_hex(20),'status':1,'isVisible':1,'userSource':1,'userRoleList':['ROLE_USER']})
if not any(s['name']==service for s in api('GET','/service/public/v2/api/service')):
    api('POST','/service/public/v2/api/service',{'name':service,'type':'hive','isEnabled':True,'configs':{'username':'lyb','password':'unused','jdbc.driverClassName':'org.apache.hive.jdbc.HiveDriver','jdbc.url':'jdbc:hive2://localhost:10000','policy.download.auth.users':'lyb,alice,bob,bench_full,denied'}})
policies=api('GET','/service/public/v2/api/policy?serviceName='+service);names={p['name'] for p in policies}
resource={'database':{'values':['nyc']},'table':{'values':['taxi_trips']},'column':{'values':['VendorID','trip_distance','fare_amount','payment_type']}}
desired=[{'name':'bench-four-columns','policyType':0,'resources':resource,'policyItems':[{'users':['alice','bob','bench_full'],'accesses':[{'type':'select','isAllowed':True}]}]}]
desired.append({'name':'bench-row-policy','policyType':2,'resources':{k:v for k,v in resource.items() if k!='column'},'rowFilterPolicyItems':[{'users':[user],'accesses':[{'type':'select','isAllowed':True}],'rowFilterInfo':{'filterExpr':'VendorID = '+str(vendor)}} for user,vendor in [('alice',1),('bob',2)]]})
for p in desired:
    p.update(service=service,isEnabled=True,isAuditEnabled=True)
    same=next((x for x in policies if x['name']==p['name'] or (p['policyType']==2 and x.get('policyType')==2)),None)
    if same:api('PUT','/service/public/v2/api/policy/'+str(same['id']),dict(same,**p))
    else:api('POST','/service/public/v2/api/policy',p)
policies=api('GET','/service/public/v2/api/policy?serviceName='+service)
(r/'runs/native-baseline-0915/ranger-policies.json').write_text(json.dumps(policies,indent=2));print('Native benchmark policies ready')
