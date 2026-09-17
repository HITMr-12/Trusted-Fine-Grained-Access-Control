# -*- coding: utf-8 -*-
"""TPC-DS store_sales 三模式可用性验证执行器。
用法:
  spark-submit ... validate_tpcds_bench.py NATIVE <user> cases.json results.jsonl
  spark-submit ... validate_tpcds_bench.py INLINE lyb cases.json results.jsonl
  spark-submit ... validate_tpcds_bench.py FGAC lyb cases.json results.jsonl
NATIVE: 每个主体单独提交（HADOOP_USER_NAME=<user>），Ranger 行过滤自动生效。
INLINE: 单提交跑全部 case，行过滤内联进 WHERE（无 Ranger 插件）。
FGAC : 单提交跑全部 case，FlightBridge 携带 case 主体 token 访问 Remote。
所有路径以相同的 xxhash64 双盐摘要汇收尾，供跨模式一致性比对。
"""
import json, os, sys, time, traceback
from pathlib import Path

mode, user, cases_path, out_path = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
cases = json.loads(Path(cases_path).read_text())

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName(f'tpcds-validate-{mode}-{user}').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
COLS = ["ss_item_sk", "ss_store_sk", "ss_quantity", "ss_sales_price", "ss_net_paid"]
TABLE = "fgac.tpcds.store_sales"
COL_SQL = ", ".join(COLS)
results = []


def digest_of(df):
    cols = [F.col(c) for c in df.columns]
    return (df.select(F.xxhash64(*cols).cast('decimal(38,0)').alias('h1'),
                      F.xxhash64(F.lit(9173), *cols).cast('decimal(38,0)').alias('h2'))
             .agg(F.count('*').alias('rows'), F.sum('h1').alias('sum1'), F.sum('h2').alias('sum2')))


def run_case(case):
    bridge = None
    try:
        t = time.perf_counter()
        if mode == 'FGAC':
            from flight_bridge_tpcds import FlightBridge
            bridge = FlightBridge(case['plan'], 'Bearer ' + case['principal'] + '-token', prefetch=True)
            df = bridge.dataframe(spark)
        else:
            business = case['business_sql']
            if mode == 'INLINE' and case['row_filter_sql']:
                where = case['row_filter_sql'] + ' AND (' + business + ')'
            else:
                where = business
            df = spark.sql(f'SELECT {COL_SQL} FROM {TABLE} WHERE {where}')
        value = {k: str(v) for k, v in digest_of(df).first().asDict().items()}
        schema = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
        if bridge:
            bridge.finish()
        return {'ok': True, 'case': case['name'], 'mode': mode, 'principal': case['principal'],
                'rows': int(value['rows']), 'sum1': value['sum1'], 'sum2': value['sum2'],
                'schema': schema, 'ms': round((time.perf_counter() - t) * 1000, 1)}
    except Exception:
        return {'ok': False, 'case': case['name'], 'mode': mode, 'principal': case['principal'],
                'error': traceback.format_exc()[-800:]}
    finally:
        if bridge:
            try:
                bridge.close()
            except Exception:
                pass


try:
    for case in cases:
        if mode == 'NATIVE' and case['principal'] != user:
            continue
        row = run_case(case)
        results.append(row)
        print(('RESULT ' if row['ok'] else 'FAIL ') + json.dumps(row, ensure_ascii=False), flush=True)
finally:
    spark.stop()
    with open(out_path, 'a') as f:
        for row in results:
            f.write(json.dumps(row, ensure_ascii=False) + '\n')
