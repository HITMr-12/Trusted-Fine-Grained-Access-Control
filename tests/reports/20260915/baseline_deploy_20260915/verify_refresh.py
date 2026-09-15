import json,time
from pathlib import Path
from pyspark.sql import SparkSession
r=Path('/home/lyb/fgac-lab')
api_scope={};exec((r/'deploy/configure_authz.py').read_text().split('users =')[0],api_scope);api=api_scope['api']
policies=api('GET','/service/public/v2/api/policy?serviceName=fgac_spark');policy=next(p for p in policies if p['name']=='lab-alice-row-filter');original=json.loads(json.dumps(policy))
s=SparkSession.builder.appName('ranger-live-policy-refresh').getOrCreate();result={'ok':False}
def ids():return [x.VendorID for x in s.sql('SELECT VendorID FROM fgac.baseline.authz_fixture ORDER BY amount').collect()]
try:
    assert ids()==[1,1]
    policy['rowFilterPolicyItems'][0]['rowFilterInfo']['filterExpr']='VendorID = 2';api('PUT','/service/public/v2/api/policy/'+str(policy['id']),policy)
    start=time.monotonic()
    while time.monotonic()-start<40:
        time.sleep(2)
        if ids()==[2]:break
    else:raise AssertionError('Updated policy did not take effect')
    result['observed_refresh_seconds']=round(time.monotonic()-start,3)
finally:
    api('PUT','/service/public/v2/api/policy/'+str(original['id']),original)
    start=time.monotonic()
    while time.monotonic()-start<40:
        time.sleep(2)
        if ids()==[1,1]:result['restored']=True;break
    result['ok']='observed_refresh_seconds' in result and result.get('restored',False)
    (r/'runs/ranger-policy-refresh.json').write_text(json.dumps(result,indent=2));s.stop()
assert result['ok'],result
