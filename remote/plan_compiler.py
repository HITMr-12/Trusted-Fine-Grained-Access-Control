import re
from pathlib import Path
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, functions as F


class PlanValidationError(ValueError):
    pass


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ALLOWED_RELATIONS = {"lake.sales.orders"}
ALLOWED_PLAN_OPERATORS = {"governed_scan", "filter", "project"}
ALLOWED_EXPRESSION_OPERATORS = {
    "column",
    "literal",
    "eq",
    "neq",
    "gt",
    "gte",
    "lt",
    "lte",
    "and",
    "or",
    "not",
    "is_null",
    "is_not_null",
}


class PlanCompiler:
    def __init__(
        self,
        session: SparkSession,
        raw_root: Path,
        policy_loader: Callable[[str], dict],
        source_loader: Callable[[str], Path] | None = None,
    ):
        self.session = session
        self.raw_root = raw_root
        self.policy_loader = policy_loader
        self.source_loader = source_loader
        self.audit = {"operators": [], "policy_versions": {}}

    def compile(self, root: dict) -> tuple[DataFrame, dict]:
        self._validate_shape(root, depth=0)
        result = self._compile_plan(root)
        return result, self.audit

    def _validate_shape(self, node: object, depth: int):
        if depth > 32:
            raise PlanValidationError("Plan exceeds maximum depth")
        if not isinstance(node, dict):
            raise PlanValidationError("Every plan node must be an object")
        operator = node.get("op")
        if operator not in ALLOWED_PLAN_OPERATORS:
            raise PlanValidationError(f"Unsupported plan operator: {operator}")
        allowed_fields = {
            "governed_scan": {"op", "relation"},
            "filter": {"op", "condition", "input"},
            "project": {"op", "columns", "input"},
        }[operator]
        extra = set(node) - allowed_fields
        if extra:
            raise PlanValidationError(f"Unexpected plan fields: {sorted(extra)}")
        if operator in {"filter", "project"}:
            self._validate_shape(node.get("input"), depth + 1)
        if operator == "filter":
            self._validate_expression(node.get("condition"), depth + 1)
        if operator == "project":
            columns = node.get("columns")
            if not isinstance(columns, list) or not columns or len(columns) > 256:
                raise PlanValidationError("Project columns must be a non-empty list")
            if columns != ["*"] and not all(
                isinstance(c, str) and IDENTIFIER.fullmatch(c) for c in columns
            ):
                raise PlanValidationError("Project contains an invalid column")

    def _validate_expression(self, expression: object, depth: int):
        if depth > 32 or not isinstance(expression, dict):
            raise PlanValidationError("Invalid expression tree")
        operator = expression.get("op")
        if operator not in ALLOWED_EXPRESSION_OPERATORS:
            raise PlanValidationError(f"Unsupported expression operator: {operator}")
        if operator == "column":
            if set(expression) != {"op", "name"} or not IDENTIFIER.fullmatch(
                str(expression.get("name", ""))
            ):
                raise PlanValidationError("Invalid column expression")
        elif operator == "literal":
            if set(expression) != {"op", "data_type", "value"}:
                raise PlanValidationError("Invalid literal expression")
            if expression["data_type"] not in {"string", "long", "double", "boolean", "null"}:
                raise PlanValidationError("Unsupported literal type")
        elif operator in {"not", "is_null", "is_not_null"}:
            if set(expression) != {"op", "input"}:
                raise PlanValidationError("Invalid unary expression")
            self._validate_expression(expression["input"], depth + 1)
        else:
            if set(expression) != {"op", "left", "right"}:
                raise PlanValidationError("Invalid binary expression")
            self._validate_expression(expression["left"], depth + 1)
            self._validate_expression(expression["right"], depth + 1)

    def _compile_plan(self, node: dict) -> DataFrame:
        operator = node["op"]
        self.audit["operators"].append(operator)
        if operator == "governed_scan":
            return self._compile_governed_scan(node["relation"])
        child = self._compile_plan(node["input"])
        if operator == "filter":
            return child.filter(self._compile_expression(node["condition"]))
        if node["columns"] == ["*"]:
            return child
        unknown = set(node["columns"]) - set(child.columns)
        if unknown:
            raise PlanValidationError(f"Unknown projected columns: {sorted(unknown)}")
        return child.select(*node["columns"])

    def _compile_governed_scan(self, relation: str) -> DataFrame:
        if relation not in ALLOWED_RELATIONS:
            raise PlanValidationError("Relation is not remotely governable")
        source = self.source_loader(relation) if self.source_loader else self.raw_root / relation
        policy = self.policy_loader(relation)
        self.audit["policy_versions"][relation] = policy["version"]
        if isinstance(source, str):
            raw_df = self.session.table(source)
        else:
            if not source.is_file():
                raise PlanValidationError("Raw relation not found")
            raw_df = self.session.read.parquet(str(source))
        governed_df = self._apply_row_policy(raw_df, policy["row_filter"])
        return self._apply_masks(governed_df, policy.get("masks", {}))

    def _apply_row_policy(self, df: DataFrame, row: dict) -> DataFrame:
        if row.get("op") != "eq" or row.get("column") not in df.columns:
            raise PlanValidationError("Unsupported catalog row policy")
        return df.filter(F.col(row["column"]) == F.lit(row["value"]))

    def _apply_masks(self, df: DataFrame, masks: dict) -> DataFrame:
        for column, mask in masks.items():
            if column not in df.columns or mask.get("type") != "last4":
                raise PlanValidationError("Unsupported catalog mask")
            df = df.withColumn(
                column,
                F.concat(
                    F.lit(mask.get("prefix", "****")),
                    F.substring(F.col(column).cast("string"), -4, 4),
                ),
            )
        return df

    def _compile_expression(self, expression: dict):
        operator = expression["op"]
        if operator == "column":
            return F.col(expression["name"])
        if operator == "literal":
            return F.lit(expression["value"])
        if operator == "not":
            return ~self._compile_expression(expression["input"])
        if operator == "is_null":
            return self._compile_expression(expression["input"]).isNull()
        if operator == "is_not_null":
            return self._compile_expression(expression["input"]).isNotNull()
        left = self._compile_expression(expression["left"])
        right = self._compile_expression(expression["right"])
        return {
            "eq": lambda: left == right,
            "neq": lambda: left != right,
            "gt": lambda: left > right,
            "gte": lambda: left >= right,
            "lt": lambda: left < right,
            "lte": lambda: left <= right,
            "and": lambda: left & right,
            "or": lambda: left | right,
        }[operator]()
