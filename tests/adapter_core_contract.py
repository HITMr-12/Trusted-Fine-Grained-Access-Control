import requests
BASE="http://adapter-core:8004"
PLAN={"version":2,"schema_version":"1","root":{"op":"project","columns":["id","region","amount","owner","card_no"],"input":{"op":"governed_scan","relation":"lake.sales.orders"}}}
H={"Authorization":"Bearer alice-token"}
requests.post(BASE+"/metrics/reset",timeout=5).raise_for_status()
a=requests.post(BASE+"/v1/materializations",headers=H,json={"query_id":"q-e2e-1","plan":PLAN},timeout=30).json()
b=requests.post(BASE+"/v1/materializations",headers=H,json={"query_id":"q-e2e-1","plan":PLAN},timeout=30).json()
assert a["remote_executed"] and not b["remote_executed"]
assert requests.get(BASE+"/v1/materializations/"+a["materialization_id"],headers=H,timeout=5).json()["inline_rows"]
assert requests.get(BASE+"/v1/materializations/"+a["materialization_id"],headers={"Authorization":"Bearer bob-token"},timeout=5).status_code==403
m=requests.get(BASE+"/metrics",timeout=5).json(); assert m["remote_materialize_calls"]==1 and m["local_rescans"]==1,m
print("PASS adapter core execute-once/local-rescan/principal-isolation",m)
