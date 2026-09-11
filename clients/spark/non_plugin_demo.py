import json
import os
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def read_virtual_table(token):
    relation = urllib.parse.quote("lake.sales.orders", safe=".")
    url = (
        os.getenv("REMOTE_URL", "http://remote:8002")
        + f"/v2/virtual-tables/{relation}?columns=id,region,amount,owner"
    )
    request = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {token}"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


spark = (
    SparkSession.builder.master("local[2]")
    .appName("fgac-pure-spark-non-plugin-demo")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

orders_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("region", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("owner", StringType(), True),
])

departments_path = os.path.join(
    os.getenv("PUBLIC_STORAGE_ROOT", "/local-data"), "departments.csv"
)
departments = (
    spark.read.option("header", True).option("inferSchema", True).csv(departments_path)
)
departments.createOrReplaceTempView("departments")

query = """SELECT d.department, SUM(o.amount) AS total
FROM orders o JOIN departments d ON o.owner = d.owner
WHERE o.amount >= 1000 AND d.enabled = true
GROUP BY d.department ORDER BY d.department"""

expected = {
    "alice-token": [("engineering", 1000), ("finance", 3000)],
    "bob-token": [("finance", 2000)],
}

print("MODE: non-plugin virtual table; no spark.sql.extensions configured")
for token, wanted in expected.items():
    response = read_virtual_table(token)
    assert response["access_mode"] == "virtual_table"
    rows = response["inline_rows"]
    spark.createDataFrame(rows, orders_schema).createOrReplaceTempView("orders")
    result = spark.sql(query)
    actual = [(row.department, row.total) for row in result.collect()]
    print(f"PRINCIPAL={response['principal']} POLICY={response['policy_version']}")
    result.explain(mode="formatted")
    result.show(truncate=False)
    assert actual == wanted, f"{response['principal']}: expected {wanted}, got {actual}"

spark.stop()
