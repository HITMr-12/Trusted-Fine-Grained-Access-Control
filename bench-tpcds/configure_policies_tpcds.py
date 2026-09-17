# -*- coding: utf-8 -*-
"""TPC-DS store_sales 基准策略：在既有服务 fgac_bench_0915 上【附加】策略，不动 0915 taxi 旧策略。
幂等：重复执行得到相同终态（按 name 匹配后 PUT 覆盖）。
运行: python3 configure_policies_tpcds.py  (E 节点, 依赖 /home/lyb/fgac-lab/deploy/configure_authz.py 的 api 助手)
"""
import json
from pathlib import Path

r = Path('/home/lyb/fgac-lab')
scope = {}
exec((r / 'deploy/configure_authz.py').read_text().split('users =')[0], scope)
api = scope['api']
service = 'fgac_bench_0915'
db, tbl = 'tpcds', 'store_sales'
columns = ['ss_item_sk', 'ss_store_sk', 'ss_quantity', 'ss_sales_price', 'ss_net_paid']

# 确保测试主体存在（0915 基线已建 alice/bob/bench_full/denied，这里幂等补齐）
users = {u['name'] for u in api('GET', '/service/xusers/users?pageSize=1000')['vXUsers']}
for name in ['alice', 'bob', 'bench_full', 'denied']:
    if name not in users:
        import secrets
        api('POST', '/service/xusers/users', {'name': name, 'firstName': name,
            'password': 'Lab9_' + secrets.token_hex(20), 'status': 1, 'isVisible': 1,
            'userSource': 1, 'userRoleList': ['ROLE_USER']})

desired = [
    {   # 列级 SELECT 授权：五个可释放列，其余列（含 ss_ticket_number）不可见
        'name': 'tpcds-store_sales-five-columns',
        'policyType': 0,
        'resources': {'database': {'values': [db]}, 'table': {'values': [tbl]},
                      'column': {'values': columns}},
        'policyItems': [{'users': ['alice', 'bob', 'bench_full'],
                         'accesses': [{'type': 'select', 'isAllowed': True}]}],
    },
    {   # 行过滤：alice 仅门店 1，bob 仅门店 2；bench_full 无行策略（走全量）
        'name': 'tpcds-store_sales-row-filter',
        'policyType': 2,
        'resources': {'database': {'values': [db]}, 'table': {'values': [tbl]}},
        'rowFilterPolicyItems': [
            {'users': ['alice'], 'accesses': [{'type': 'select', 'isAllowed': True}],
             'rowFilterInfo': {'filterExpr': 'ss_store_sk = 1'}},
            {'users': ['bob'], 'accesses': [{'type': 'select', 'isAllowed': True}],
             'rowFilterInfo': {'filterExpr': 'ss_store_sk = 2'}},
        ],
    },
]

policies = api('GET', '/service/public/v2/api/policy?serviceName=' + service)
for p in desired:
    p.update(service=service, isEnabled=True, isAuditEnabled=True)
    same = next((x for x in policies if x['name'] == p['name']
                 or (p['policyType'] == 2 and x.get('policyType') == 2
                     and x.get('resources', {}).get('table', {}).get('values') == [tbl])), None)
    if same:
        api('PUT', '/service/public/v2/api/policy/' + str(same['id']), dict(same, **p))
        print('updated', p['name'])
    else:
        api('POST', '/service/public/v2/api/policy', p)
        print('created', p['name'])

policies = api('GET', '/service/public/v2/api/policy?serviceName=' + service)
(r / 'runs/tpcds-bench').mkdir(parents=True, exist_ok=True)
(r / 'runs/tpcds-bench/ranger-policies-after.json').write_text(json.dumps(policies, indent=2))
tpcds = [p for p in policies if tbl in json.dumps(p.get('resources', {}))]
print(f'TPC-DS policies active: {len(tpcds)} (service {service})')
