import json
import urllib.error
import urllib.request

REMOTE = "http://remote:8002/v2/subplans"


def request(plan, token="alice-token"):
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        REMOTE, data=json.dumps(plan).encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def scan(columns=None, schema_version="1"):
    return {
        "version": 2,
        "schema_version": schema_version,
        "root": {
            "op": "project",
            "columns": columns or ["owner", "amount"],
            "input": {"op": "governed_scan", "relation": "lake.sales.orders"},
        },
    }


checks = [
    ("missing token", request(scan(), None)[0], 401),
    ("forged schema version", request(scan(schema_version="999"))[0], 409),
    ("forbidden column", request(scan(["owner", "secret_payload"]))[0], 403),
    ("unapproved operator", request({
        "version": 2, "schema_version": "1",
        "root": {"op": "export_raw", "input": {
            "op": "governed_scan", "relation": "lake.sales.orders"}},
    })[0], 403),
]

for name, actual, expected in checks:
    assert actual == expected, f"{name}: expected {expected}, got {actual}"
    print(f"PASS {name}: HTTP {actual}")

status, body = request(scan())
assert status == 200 and body["principal"] == "alice"
assert body["inline_rows"] == [["alice", 1000], ["bob", 3000]]
print("PASS authorized plan: governed rows only")
