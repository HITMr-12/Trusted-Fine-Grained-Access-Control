"""Arrow-raw byte-pipe bridge, E side, 2-stream edition. The R endpoint produces
TWO complete independent Arrow IPC byte streams and interleaves binary frames
tagged with a 1-byte stream id; here each frame payload is routed to its own
socket verbatim (bytes 1..n), so each socket receives exactly one valid IPC
stream. Zero parsing and zero RecordBatch materialization on E."""
import json, os, secrets, socket, threading, time
import pyarrow.flight as flight


class ArrowRawBridge:
    def __init__(self, plan, token):
        url = os.getenv('FGAC_FLIGHT_URL', 'grpc://172.168.22.23:18839')
        self.client = flight.FlightClient(
            url, generic_options=[(b'grpc.max_receive_message_length', -1),
                                  (b'grpc.max_send_message_length', -1)])
        self.options = flight.FlightCallOptions(
            timeout=600, headers=[(b'authorization', token.encode())])
        descriptor = flight.FlightDescriptor.for_command(json.dumps(plan).encode())
        self.info = self.client.get_flight_info(descriptor, self.options)
        if len(self.info.endpoints) != 1:
            raise RuntimeError('This client currently supports one scan endpoint')
        self.secret = secrets.token_hex(32)
        self.listeners = []
        for _ in range(2):
            s = socket.socket()
            s.bind(('127.0.0.1', 0))
            s.listen(1)
            s.settimeout(120)
            self.listeners.append(s)
        self.connections = []
        self.nbytes = 0
        self.batches = 0
        self.rows = 0
        self.pipe_write_ms = 0.
        self.first_batch_at = None
        self.error = None
        self.thread = threading.Thread(target=self.pump, daemon=True)
        self.thread.start()

    def _accept(self, listener):
        conn, _ = listener.accept()
        conn.settimeout(120)
        self.connections.append(conn)
        got = b''
        while len(got) < 64:
            part = conn.recv(64 - len(got))
            if not part:
                raise RuntimeError('Socket authentication ended early')
            got += part
        if not secrets.compare_digest(got, self.secret.encode()):
            raise RuntimeError('Bad local nonce')
        return conn.makefile('wb')

    def pump(self):
        streams = []
        try:
            for listener in self.listeners:
                streams.append(self._accept(listener))
            reader = self.client.do_get(self.info.endpoints[0].ticket, self.options)
            for chunk in reader:
                if chunk.data is None:
                    continue
                if self.first_batch_at is None:
                    self.first_batch_at = time.perf_counter()
                t = time.perf_counter()
                for cell in chunk.data.column(0).to_pylist():
                    if cell:
                        streams[cell[0]].write(cell[1:])
                        self.nbytes += len(cell)
                for stream in streams:
                    stream.flush()
                self.pipe_write_ms += (time.perf_counter() - t) * 1000
                self.batches += 1
            try:
                reader.cancel()
            except Exception:
                pass
            for stream in streams:
                stream.flush()
                stream.close()
        except BaseException as exc:
            self.error = exc
        finally:
            for listener in self.listeners:
                try:
                    listener.close()
                except OSError:
                    pass

    def dataframe(self, spark, spark_schema=None):
        from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DecimalType
        if spark_schema is None:
            spark_schema = StructType([StructField('ss_item_sk', LongType()),
                                       StructField('ss_store_sk', LongType()),
                                       StructField('ss_quantity', IntegerType()),
                                       StructField('ss_sales_price', DecimalType(7, 2)),
                                       StructField('ss_net_paid', DecimalType(7, 2))])
        return spark.read.format('fgac.bench.ArrowSocketSource') \
            .option('schema', spark_schema.json()) \
            .option('ports', ','.join(str(x.getsockname()[1]) for x in self.listeners)) \
            .option('secret', self.secret).load()

    def finish(self):
        self.thread.join(600)
        if self.thread.is_alive():
            raise RuntimeError('Arrow raw bridge did not finish')
        if self.error:
            raise RuntimeError('Arrow raw bridge failed') from self.error

    def close(self):
        try:
            self.client.close()
        except Exception:
            pass
        for conn in self.connections:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conn.close()
        for listener in self.listeners:
            try:
                listener.close()
            except OSError:
                pass
        self.thread.join(5)
