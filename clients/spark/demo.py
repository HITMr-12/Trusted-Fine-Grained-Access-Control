import os
import json
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

USER_SQL = os.getenv("USER_SQL", """SELECT d.department, SUM(o.amount) AS total
FROM orders o JOIN departments d ON o.owner = d.owner
WHERE o.amount >= 1000 AND d.enabled = true
GROUP BY d.department ORDER BY d.department""")

spark = (SparkSession.builder.master("local[2]")
         .appName("fgac-pure-spark-plugin-demo")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("region", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("owner", StringType(), True),
    StructField("card_no", StringType(), True),
])
spark.createDataFrame([], schema).createOrReplaceTempView("orders")
with urllib.request.urlopen(
        os.getenv("POLARIS_URL", "http://polaris-fgac:8181") +
        "/v2/relations/public/departments", timeout=5) as response:
    departments_metadata = json.load(response)
if departments_metadata.get("access_mode") != "direct":
    raise RuntimeError("Catalog did not allocate departments for direct access")
storage_uri = departments_metadata["storage_uri"]
if not storage_uri.startswith("public://") or "/" in storage_uri.removeprefix("public://"):
    raise RuntimeError("Unsupported Catalog public storage URI")
departments_path = os.path.join(
    os.getenv("PUBLIC_STORAGE_ROOT", "/local-data"),
    storage_uri.removeprefix("public://"))
(spark.read.option("header", True).option("inferSchema", True)
 .csv(departments_path)
 .createOrReplaceTempView("departments"))
print("ORIGINAL SQL (submitted once):\n" + USER_SQL)
result = spark.sql(USER_SQL)
result.explain(mode="extended")
result.show(truncate=False)
spark.stop()
