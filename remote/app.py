import os
import csv
import io
import tempfile
from pathlib import Path

import requests
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict
from pyspark.sql import SparkSession

from remote.plan_compiler import PlanCompiler, PlanValidationError
from remote.cost_estimator import estimate

app = FastAPI(title="Stable secure FGAC remote")
POLARIS_URL = os.getenv("POLARIS_URL", "http://polaris-fgac:8181")
STORAGE_URL = os.getenv("STORAGE_URL", "http://storage:8003")
STORAGE_TOKEN = os.getenv("STORAGE_TOKEN", "remote-storage-secret")


class SubplanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: int
    schema_version: str
    root: dict


def spark() -> SparkSession:
    builder = (SparkSession.builder.master("local[2]").appName("secure-fgac-remote")
            .config("spark.ui.enabled", "false"))
    if os.getenv("FGAC_ICEBERG_TABLE"):
        jars = os.getenv("FGAC_REMOTE_JARS", "")
        builder = (builder.config("spark.jars", jars)
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.fgac", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.fgac.type", "hadoop")
            .config("spark.sql.catalog.fgac.warehouse", os.getenv("FGAC_ICEBERG_WAREHOUSE"))
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("FGAC_S3A_ENDPOINT"))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("FGAC_S3A_ACCESS_KEY"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("FGAC_S3A_SECRET_KEY"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    return builder.getOrCreate()


def inspect_plan(node: dict) -> tuple[str, list[str], list[str]]:
    operators, projected = [], ["id", "region", "amount", "owner", "card_no"]
    current = node
    relation = None
    while isinstance(current, dict):
        op = current.get("op")
        operators.append(op)
        if op == "project": projected = current.get("columns", [])
        if op == "governed_scan":
            relation = current.get("relation")
            break
        current = current.get("input")
    if not relation: raise HTTPException(status_code=400, detail="Missing governed scan")
    return relation, operators, projected


def authorize(token: str, relation: str, operators: list[str], columns: list[str], version: str):
    response = requests.post(f"{POLARIS_URL}/v2/authorize",
        headers={"Authorization": token}, timeout=5,
        json={"relation_id": relation, "requested_operators": operators,
              "requested_columns": columns, "schema_version": version})
    if response.status_code // 100 != 2:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()


def fetch_source(contract: dict):
    table = os.getenv("FGAC_ICEBERG_TABLE")
    if table:
        return table
    response = requests.get(f"{STORAGE_URL}/v1/objects/{contract['storage_object']}",
        headers={"Authorization": f"Bearer {STORAGE_TOKEN}"}, timeout=15)
    response.raise_for_status()
    handle = tempfile.NamedTemporaryFile(prefix="fgac-", suffix=".parquet", delete=False)
    handle.write(response.content); handle.close()
    return Path(handle.name)


@app.get("/health")
def health(): return {"status": "ok"}


def materialize_result(governed_df, query_id: str) -> dict:
    """Write the governed result as Parquet under the query-scoped results
    prefix and return a per-query credential envelope (TTL, read-only)."""
    import uuid
    results_root = os.getenv("FGAC_RESULT_ROOT", "s3a://fgac/results")
    object_key = f"{uuid.uuid4().hex}-{query_id}"
    path = f"{results_root}/{object_key}"
    governed_df.write.mode("overwrite").parquet(path)
    ttl = int(os.getenv("FGAC_RESULT_TTL_SECONDS", "900"))
    return {"result_uri": path,
            "storage_endpoint": os.getenv("FGAC_S3A_ENDPOINT_EXTERNAL", "http://172.168.22.25:9100"),
            "access_key": os.getenv("FGAC_RESULT_ACCESS_KEY", "fgacadmin"),
            "secret_key": os.getenv("FGAC_RESULT_SECRET_KEY",
                                    "fgac-minio-secret-2025"),
            "ttl_seconds": ttl, "expires_hint": ttl}


@app.post("/v2/subplans")
def execute_subplan(request: SubplanRequest, authorization: str | None = Header(default=None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Bearer token required")
    if request.version != 2: raise HTTPException(status_code=400, detail="Unsupported protocol")
    relation, operators, columns = inspect_plan(request.root)
    contract = authorize(authorization, relation, operators, columns, request.schema_version)
    source = fetch_source(contract)
    cleanup = isinstance(source, Path)
    try:
        compiler = PlanCompiler(spark(), Path("/unused"),
            policy_loader=lambda _: contract["policy"], source_loader=lambda _: source)
        governed_df, audit = compiler.compile(request.root)
        if os.getenv("FGAC_RESULT_MODE", "parquet") == "parquet":
            envelope = materialize_result(governed_df, relation)
            envelope.update({"status": "SUCCEEDED", "principal": contract["principal"],
                "schema_version": contract["schema_version"],
                "policy_version": contract["policy"]["version"],
                "output_schema": [{"name": f.name, "type": f.dataType.simpleString(),
                                   "nullable": f.nullable} for f in governed_df.schema.fields],
                "executed_operators": audit["operators"]})
            return envelope
        rows = [[row[field.name] for field in governed_df.schema.fields]
                for row in governed_df.collect()]
        return {"status": "SUCCEEDED", "principal": contract["principal"],
            "schema_version": contract["schema_version"],
            "policy_version": contract["policy"]["version"],
            "output_schema": [{"name": f.name, "type": f.dataType.simpleString(),
                               "nullable": f.nullable} for f in governed_df.schema.fields],
            "inline_rows": rows, "executed_operators": audit["operators"]}
    except PlanValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if cleanup:
            Path(source).unlink(missing_ok=True)


@app.post("/v2/subplans/estimate")
def estimate_subplan(request: SubplanRequest, method: str = Query(default="policy_sample"),
                     authorization: str | None = Header(default=None)):
    """Return an optimizer-facing cost envelope without returning protected rows."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Bearer token required")
    relation, operators, columns = inspect_plan(request.root)
    contract = authorize(authorization, relation, operators, columns, request.schema_version)
    source = fetch_source(contract)
    cleanup = isinstance(source, Path)
    try:
        compiler = PlanCompiler(spark(), Path("/unused"),
            policy_loader=lambda _: contract["policy"], source_loader=lambda _: source)
        governed_df, _ = compiler.compile(request.root)
        base_rows = governed_df.count()
        envelope = estimate(governed_df, method, base_rows).to_dict()
    except (PlanValidationError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if cleanup:
            Path(source).unlink(missing_ok=True)
    return {"relation": relation, "principal": contract["principal"],
            "policy_version": contract["policy"]["version"], "cost_envelope": envelope}


@app.get("/v2/virtual-tables/{relation:path}")
def read_virtual_table(
    relation: str,
    columns: str = Query(default="id,region,amount,owner,card_no"),
    output_format: str = Query(default="json", alias="format"),
    authorization: str | None = Header(default=None),
):
    """Non-plugin fallback: expose a Catalog-governed relation as a virtual table.

    The engine supplies only a bearer token, relation and projection. Principal and
    row policy still come from Catalog through the normal subplan authorization path.
    """
    projected = [column.strip() for column in columns.split(",") if column.strip()]
    if not projected:
        raise HTTPException(status_code=400, detail="At least one column is required")
    result = execute_subplan(
        SubplanRequest(
            version=2,
            schema_version="1",
            root={
                "op": "project",
                "columns": projected,
                "input": {"op": "governed_scan", "relation": relation},
            },
        ),
        authorization,
    )
    result["access_mode"] = "virtual_table"
    result["relation"] = relation
    if output_format == "json":
        return result
    if output_format != "csv":
        raise HTTPException(status_code=400, detail="format must be json or csv")
    stream = io.StringIO()
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow([field["name"] for field in result["output_schema"]])
    writer.writerows(result["inline_rows"])
    return Response(
        stream.getvalue(),
        media_type="text/csv",
        headers={
            "X-FGAC-Principal": result["principal"],
            "X-FGAC-Policy-Version": str(result["policy_version"]),
        },
    )
