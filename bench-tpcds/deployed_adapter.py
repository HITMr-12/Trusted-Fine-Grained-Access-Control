"""Deployment wiring: existing compiler/authorization, Polaris Spark config.
2026-09-16: fetch_source 支持按受控关系选择源表（FGAC_TABLE_<RELATION> 环境变量），
原 FGAC_ICEBERG_TABLE 兜底行为不变。"""
import os, re, requests, tempfile
from pathlib import Path
class HTTPException(Exception):
    def __init__(self,status_code,detail):
        self.status_code=status_code
        self.detail=detail
        super().__init__(str(status_code))
POLARIS_URL=os.environ["POLARIS_URL"]
STORAGE_URL="unused"
STORAGE_TOKEN="unused"

def inspect_plan(node: dict) -> tuple[str, list[str], list[str]]:
    operators, projected = ([], ['id', 'region', 'amount', 'owner', 'card_no'])
    current = node
    relation = None
    while isinstance(current, dict):
        op = current.get('op')
        operators.append(op)
        if op == 'project':
            projected = current.get('columns', [])
        if op == 'governed_scan':
            relation = current.get('relation')
            break
        current = current.get('input')
    if not relation:
        raise HTTPException(status_code=400, detail='Missing governed scan')
    return (relation, operators, projected)

def authorize(token: str, relation: str, operators: list[str], columns: list[str], version: str):
    response = requests.post(f'{POLARIS_URL}/v2/authorize', headers={'Authorization': token}, timeout=5, json={'relation_id': relation, 'requested_operators': operators, 'requested_columns': columns, 'schema_version': version})
    if response.status_code // 100 != 2:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()

def fetch_source(contract: dict):
    relation = contract.get('relation_id', '')
    specific = os.getenv('FGAC_TABLE_' + re.sub(r'[^A-Z0-9]', '_', relation.upper()))
    table = specific or os.getenv('FGAC_ICEBERG_TABLE')
    if table:
        return table
    response = requests.get(f"{STORAGE_URL}/v1/objects/{contract['storage_object']}", headers={'Authorization': f'Bearer {STORAGE_TOKEN}'}, timeout=15)
    response.raise_for_status()
    handle = tempfile.NamedTemporaryFile(prefix='fgac-', suffix='.parquet', delete=False)
    handle.write(response.content)
    handle.close()
    return Path(handle.name)
