"""Spark 3.5 Arrow socket adapter. Does not build a list of result batches.

This deliberately uses a private Spark API. Batch arrival order is unspecified;
only the existing unordered scan/filter/project plans are supported.
"""
import pyarrow as pa


def iter_arrow_batches(frame):
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
