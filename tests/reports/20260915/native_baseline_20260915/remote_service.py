"""Deployment wiring: existing compiler/authorization, Polaris Spark config."""
from pathlib import Path
from pydantic import BaseModel,ConfigDict
from pyspark.sql import SparkSession
from deployed_adapter import authorize,fetch_source,inspect_plan
from deployed_plan_compiler import PlanCompiler as ExistingCompiler
class SubplanRequest(BaseModel):
    model_config=ConfigDict(extra='forbid')
    version:int
    schema_version:str
    root:dict
class PlanCompiler(ExistingCompiler):
    def _apply_row_policy(self,df,row):
        # Explicit catalog grant for the added whole-source benchmark principal.
        # Existing eq policies and all execution/transport code stay unchanged.
        if row=={'op':'all'}:return df
        return super()._apply_row_policy(df,row)
def spark():return SparkSession.builder.appName('fgac-current-polaris').getOrCreate()
