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
    return (SparkSession.builder.master("local[2]").appName("secure-fgac-remote")
            .config("spark.ui.enabled", "false").getOrCreate())


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


def fetch_parquet(storage_object: str) -> Path:
    response = requests.get(f"{STORAGE_URL}/v1/objects/{storage_object}",
        headers={"Authorization": f"Bearer {STORAGE_TOKEN}"}, timeout=15)
    response.raise_for_status()
    handle = tempfile.NamedTemporaryFile(prefix="fgac-", suffix=".parquet", delete=False)
    handle.write(response.content); handle.close()
    return Path(handle.name)


@app.get("/health")
def health(): return {"status": "ok"}


@app.post("/v2/subplans")
def execute_subplan(request: SubplanRequest, authorization: str | None = Header(default=None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Bearer token required")
    if request.version != 2: raise HTTPException(status_code=400, detail="Unsupported protocol")
    relation, operators, columns = inspect_plan(request.root)
    contract = authorize(authorization, relation, operators, columns, request.schema_version)
    parquet = fetch_parquet(contract["storage_object"])
    try:
        compiler = PlanCompiler(spark(), Path("/unused"),
            policy_loader=lambda _: contract["policy"], source_loader=lambda _: parquet)
        governed_df, audit = compiler.compile(request.root)
        rows = [[row[field.name] for field in governed_df.schema.fields]
                for row in governed_df.collect()]
    except PlanValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        parquet.unlink(missing_ok=True)
    return {"status": "SUCCEEDED", "principal": contract["principal"],
            "schema_version": contract["schema_version"],
            "policy_version": contract["policy"]["version"],
            "output_schema": [{"name": f.name, "type": f.dataType.simpleString(),
                               "nullable": f.nullable} for f in governed_df.schema.fields],
            "inline_rows": rows, "executed_operators": audit["operators"]}


@app.post("/v2/subplans/estimate")
def estimate_subplan(request: SubplanRequest, method: str = Query(default="policy_sample"),
                     authorization: str | None = Header(default=None)):
    """Return an optimizer-facing cost envelope without returning protected rows."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Bearer token required")
    relation, operators, columns = inspect_plan(request.root)
    contract = authorize(authorization, relation, operators, columns, request.schema_version)
    parquet = fetch_parquet(contract["storage_object"])
    try:
        compiler = PlanCompiler(spark(), Path("/unused"),
            policy_loader=lambda _: contract["policy"], source_loader=lambda _: parquet)
        governed_df, _ = compiler.compile(request.root)
        base_rows = spark().read.parquet(str(parquet)).count()
        envelope = estimate(governed_df, method, base_rows).to_dict()
    except (PlanValidationError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        parquet.unlink(missing_ok=True)
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
