"""Spark 3.5 Arrow socket adapter. Does not build a list of result batches.

This deliberately uses a private Spark API. Batch arrival order is unspecified;
only the existing unordered scan/filter/project plans are supported.
"""
import os
import socket

import pyarrow as pa


def iter_arrow_batches(frame):
    if os.environ.get("FGAC_ARROW_DELIVERY", "collect") == "partition_stream":
        yield from iter_partition_arrow_batches(frame)
        return
    from pyspark.rdd import _load_from_socket
    from pyspark.sql.pandas.serializers import ArrowCollectSerializer

    if not frame.sparkSession.version.startswith("3.5."):
        raise RuntimeError("This prototype requires Spark 3.5.x")
    port, secret, server = frame._jdf.collectAsArrowToPython()
    stream = _load_from_socket((port, secret), ArrowCollectSerializer())
    try:
        for value in stream:
            if isinstance(value, pa.RecordBatch):
                yield value
            elif not isinstance(value, list):
                raise RuntimeError("Unexpected Spark Arrow framing")
            # The last list contains batch ordering indices, not data. Our API
            # does not promise SQL ordering, so it does not buffer to reorder.
        server.getResult()  # Surface executor errors instead of a partial success.
    finally:
        if hasattr(stream, "close"):
            stream.close()
        # Cancellation is performed by the caller's Spark job group. Do not
        # block waiting for a producer after the client has stopped reading.


def iter_partition_arrow_batches(frame, probe=None):
    """Incremental, unordered delivery for Spark 3.5 local mode with helper jar.

    An error after partial delivery fails the entire scan. Callers must not commit
    partial results. Task retries are rejected to prevent duplicate delivery.
    """
    handle = frame.sparkSession._jvm.org.fgac.streaming.PartitionArrowStream.start(frame._jdf)
    connection = None
    source = None
    try:
        connection = socket.create_connection(("127.0.0.1", handle.port()), timeout=120)
        connection.sendall(handle.secret().encode("ascii"))
        source = connection.makefile("rb")
        with pa.ipc.open_stream(source) as reader:
            yield from reader
        # A truncated producer stream must never be accepted as a successful scan.
        handle.awaitComplete()
    finally:
        try:
            if probe is not None:
                probe(handle.metrics())
        finally:
            if source is not None:
                source.close()
            if connection is not None:
                connection.close()
            handle.close()
