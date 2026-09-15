import json, urllib.request, urllib.parse, urllib.error
from pathlib import Path
r=Path('/data1/lyb/fgac-lab');c=json.loads((r/'deploy/polaris-credentials.json').read_text());base='http://172.168.22.23:18182'
body=urllib.parse.urlencode({'grant_type':'client_credentials','client_id':c['client_id'],'client_secret':c['client_secret'],'scope':'PRINCIPAL_ROLE:ALL'}).encode()
with urllib.request.urlopen(urllib.request.Request(base+'/api/catalog/v1/oauth/tokens',data=body),timeout=20) as res:token=json.load(res)['access_token']
def api(method,path,body=None):
    req=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'},method=method)
    try:
        with urllib.request.urlopen(req,timeout=30) as res:
            raw=res.read();return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        print('Failed',method,path,e.code,e.read().decode()[:2000]);raise
catalogs=api('GET','/api/management/v1/catalogs')
if not any(x['name']=='fgac' for x in catalogs['catalogs']):
    api('POST','/api/management/v1/catalogs',{'catalog':{'name':'fgac','type':'INTERNAL','properties':{'default-base-location':'s3://fgac/warehouse'},'storageConfigInfo':{'storageType':'S3','allowedLocations':['s3://fgac/warehouse','s3a://fgac/warehouse'],'region':'us-east-1','endpoint':'http://172.168.22.23:19100','pathStyleAccess':True,'stsUnavailable':True}}})
prefix='/api/catalog/v1/fgac'
ns=api('GET',prefix+'/namespaces')['namespaces']
results=[]
for namespace,table in [('nyc','taxi_trips'),('baseline','authz_fixture')]:
    if [namespace] not in ns:api('POST',prefix+'/namespaces',{'namespace':[namespace],'properties':{}})
    tables=api('GET',prefix+'/namespaces/'+namespace+'/tables')['identifiers']
    if not any(t['name']==table for t in tables):
        api('POST',prefix+'/namespaces/'+namespace+'/register',{'name':table,'metadata-location':f's3a://fgac/warehouse/{namespace}/{table}/metadata/v1.metadata.json'})
    meta=api('GET',prefix+'/namespaces/'+namespace+'/tables/'+table)
    results.append({'table':f'fgac.{namespace}.{table}','metadata_location':meta['metadata-location'],'snapshot':meta['metadata'].get('current-snapshot-id')})
(r/'runs/polaris-registration.json').write_text(json.dumps(results,indent=2));print(json.dumps(results))
