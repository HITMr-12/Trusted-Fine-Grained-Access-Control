"""Persistent Spark worker for the TPC-DS store_sales bench. All measured paths end in the same digest sink."""
import json, os, sys, time, traceback, socket, uuid
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

kind, user, outdir = sys.argv[1:4]
out = Path(outdir)
s = SparkSession.builder.appName('tpcds-bench-' + kind + '-' + user).getOrCreate()
s.sparkContext.setLogLevel('ERROR')
if kind == 'NATIVE':
    extensions = s.conf.get('spark.sql.extensions', '')
    if 'org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension' not in extensions.split(','):
        s.stop()
        raise RuntimeError('NATIVE requires the Ranger Spark extension; direct-read timings are not an authorized baseline')
    # A configuration string alone does not prove Spark loaded the extension.
    try:
        s.sql('SELECT ss_ticket_number FROM fgac.tpcds.store_sales LIMIT 1').collect()
    except Exception as exc:
        if 'AccessControlException' not in str(exc):
            s.stop()
            raise RuntimeError('NATIVE authorization readiness failed') from exc
    else:
        s.stop()
        raise RuntimeError('NATIVE allowed a forbidden column; refusing benchmark')
jpid = int(s._jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split('@')[0])
hz = os.sysconf('SC_CLK_TCK')

def usage():
    stat = Path('/proc', str(jpid), 'stat').read_text().split()
    return time.process_time() + (int(stat[13]) + int(stat[14])) / hz

listener = socket.socket()
listener.bind(('127.0.0.1', 0))
listener.listen(1)
connection = None

def answer(x):
    data = json.dumps(x, default=str)
    if connection is None:
        print('BENCH_RESULT ' + data, flush=True)
    else:
        connection.sendall((data + '\n').encode())

answer({'ready': True, 'kind': kind, 'user': user, 'pid': os.getpid(), 'java': jpid,
        'spark': s.version, 'master': s.sparkContext.master,
        'cpu_affinity': sorted(os.sched_getaffinity(0)),
        'control_port': listener.getsockname()[1]})

def requests():
    global connection
    while True:
        connection, _ = listener.accept()
        connection.settimeout(300)
        line = connection.makefile('r').readline()
        yield line
        connection.close()

COLS = ["ss_item_sk", "ss_store_sk", "ss_quantity", "ss_sales_price", "ss_net_paid"]
TABLE = "fgac.tpcds.store_sales"
# MASK v2 preserves verified PLAIN source bytes and adds an independent validity bitmap.

try:
    for line in requests():
        cmd = json.loads(line)
        if cmd.get('op') == 'stop':
            break
        case = cmd['case']; mode = cmd['mode']; ident = cmd['id']; bridge = None
        try:
            s.sparkContext.setJobGroup(ident, ident)
            columns = ", ".join(COLS)
            business = case.get('business_sql', 'true')
            where = business
            if mode == 'INLINE' and case.get('row_filter_sql'):
                where = case['row_filter_sql'] + ' AND (' + business + ')'
            sql = 'SELECT ' + columns + ' FROM ' + TABLE + ' WHERE ' + where
            start_wall = time.time(); cpu = usage(); t = time.perf_counter()
            if mode in ('FGAC', 'FGAC-LZ4', 'FGAC-ZSTD'):
                from flight_bridge import FlightBridge
                sec = bool(cmd.get('sec'))
                os.environ['FGAC_FLIGHT_URL'] = (('grpc+tls' if sec else 'grpc') +
                    '://172.168.22.23:' + ('18915' if sec else '18914'))
                bridge = FlightBridge(case['plan'], cmd.get('authorization', 'Bearer ' + case['principal'] + '-token'),
                                      prefetch=True, sec=sec, tls_ca=os.environ.get('SEC_CA'))
                df = bridge.dataframe(s)
            elif mode == 'FGAC-FRAMES':
                from frame_bridge_tpcds import FrameBridge
                os.environ['FGAC_FLIGHT_URL'] = 'grpc://172.168.22.23:18836'
                bridge = FrameBridge(case['plan'], 'Bearer ' + case['principal'] + '-token')
                df = bridge.dataframe(s)
            elif mode == 'MASK':
                from mask_scan import scan_digest
                sec = bool(cmd.get('sec'))
                request = {'version': 2, 'request_id': uuid.uuid4().hex,
                           'relation_id': 'lake.sales.store_sales', 'schema_version': '1',
                           'columns': COLS, 'authorization': cmd['authorization']}
                result = scan_digest(s, os.environ['MASK_DELIVERY_ROOT'], request, business,
                    host=os.environ['MASK_HOST'], port=int('19051' if sec else '19052'),
                    ca=os.environ.get('SEC_CA'), sec=sec)
                end = time.perf_counter()
                if int(result['digest']['rows']) != case['rows']:
                    raise ValueError('MASK row count mismatch')
                answer({'ok': True, 'id': ident, 'case': case['name'], 'mode': mode,
                        'principal': case['principal'], 'rep': cmd['rep'], 'warmup': cmd['rep'] < 0,
                        'start': start_wall, 'end': time.time(), 'total_ms': (end - t) * 1000,
                        'e_cpu_ms': (usage() - cpu) * 1000, **result})
                continue
            elif mode == 'MASK3':
                # Fused variant: v2 governance/SEC intact, per-file Spark jobs
                # micro-batched and reception chunks enlarged (E-side only).
                from mask_scan_v3 import scan_digest as scan_digest_v3
                sec = bool(cmd.get('sec'))
                request = {'version': 2, 'request_id': uuid.uuid4().hex,
                           'relation_id': 'lake.sales.store_sales', 'schema_version': '1',
                           'columns': COLS, 'authorization': cmd['authorization']}
                result = scan_digest_v3(s, os.environ['MASK3_DELIVERY_ROOT'], request, business,
                    host=os.environ['MASK_HOST'],
                    port=int(cmd.get('mask_port') or ('19051' if sec else '19052')),
                    ca=os.environ.get('SEC_CA'), sec=sec)
                end = time.perf_counter()
                if int(result['digest']['rows']) != case['rows']:
                    raise ValueError('MASK3 row count mismatch')
                answer({'ok': True, 'id': ident, 'case': case['name'], 'mode': mode,
                        'principal': case['principal'], 'rep': cmd['rep'], 'warmup': cmd['rep'] < 0,
                        'start': start_wall, 'end': time.time(), 'total_ms': (end - t) * 1000,
                        'e_cpu_ms': (usage() - cpu) * 1000, **result})
                continue
            else:
                hc = s.sparkContext._jsc.hadoopConfiguration()
                if cmd.get('sec'):
                    # SEC=on: S3A over TLS (proxy in front of MinIO) + per-source-file
                    # sha256 verification pass (32 files = 1/32 row-range each), both
                    # inside the e2e timer.
                    hc.set('fs.s3a.endpoint', 'https://172.168.22.23:19102')
                    hc.set('fs.s3a.connection.ssl.enabled', 'true')
                    import hashlib as _hl
                    src_files = [r[0] for r in s.sql('SELECT file_path FROM fgac.tpcds.store_sales.files').collect()]
                    expected = {f['name']: f['sha256']
                                for f in json.load(open(os.environ['SEC_SOURCE_MANIFEST']))['files']}
                    got = (s.sparkContext.binaryFiles(','.join(src_files))
                           .map(lambda kv: (kv[0].rsplit('/', 1)[-1], _hl.sha256(kv[1]).hexdigest()))
                           .collect())
                    bad = [(n, h) for n, h in got if expected.get(n) != h]
                    if bad or len(got) != len(expected):
                        raise ValueError('SEC source verification failed: %r' % (bad[:3] or [('count', len(got))]))
                else:
                    hc.set('fs.s3a.endpoint', 'http://172.168.22.23:19100')
                    hc.set('fs.s3a.connection.ssl.enabled', 'false')
                df = s.sql(sql)
            ready = time.perf_counter()
            cols = [F.col(c) for c in df.columns]
            final = (df.select(F.xxhash64(*cols).cast('decimal(38,0)').alias('h1'),
                               F.xxhash64(F.lit(9173), *cols).cast('decimal(38,0)').alias('h2'))
                      .agg(F.count('*').alias('rows'), F.sum('h1').alias('sum1'), F.sum('h2').alias('sum2')))
            action = time.perf_counter()
            value = {k: str(v) for k, v in final.first().asDict().items()}
            if bridge:
                bridge.finish()
                bridge.close()
            end = time.perf_counter()
            cpu_ms = (usage() - cpu) * 1000
            schema = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
            assert int(value['rows']) == case['rows'], (value, case['rows'])
            row = {'ok': True, 'id': ident, 'case': case['name'], 'mode': mode,
                   'principal': case['principal'], 'rep': cmd['rep'], 'warmup': cmd['rep'] < 0,
                   'start': start_wall, 'end': time.time(),
                   'total_ms': (end - t) * 1000, 'prepare_ms': (ready - t) * 1000,
                   'digest_setup_ms': (action - ready) * 1000, 'action_ms': (end - action) * 1000,
                   'e_cpu_ms': cpu_ms, 'digest': value, 'schema': schema,
                   'sql': sql if mode in ('NATIVE', 'INLINE') else None}
            if bridge:
                row.update(stream_rows=bridge.rows, stream_batches=bridge.batches,
                           arrow_bytes=bridge.nbytes,
                           first_arrow_ms=(bridge.first_batch_at - t) * 1000 if bridge.first_batch_at else None,
                           bridge_next_wait_ms=getattr(bridge, 'next_wait_ms', None),
                           bridge_ipc_write_ms=getattr(bridge, 'ipc_write_ms', None),
                           frame_files=getattr(bridge, 'files', None) and len(bridge.files),
                           frame_disk_write_ms=getattr(bridge, 'disk_write_ms', None),
                           prefetch_peak_bytes=getattr(getattr(bridge, 'prefetched', None),
                                                       'peak_queued_bytes', None),
                           prefetch_peak_batches=getattr(getattr(bridge, 'prefetched', None),
                                                         'peak_queued_batches', None))
            if cmd.get('capture_plan'):
                (out / (case['name'] + '_' + mode + '_plan.txt')).write_text(final._jdf.queryExecution().toString())
            answer(row)
        except Exception:
            if bridge:
                try:
                    bridge.close()
                except Exception:
                    pass
            answer({'ok': False, 'id': ident, 'error': traceback.format_exc()})
finally:
    if connection:
        connection.close()
    listener.close()
    s.stop()
