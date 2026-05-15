from typing import Literal
from pydantic import BaseModel
from aidmi_pipeline.config import MigrationRun


class ExtractResult(BaseModel):
    rows_extracted: int


class DbtModelOutcome(BaseModel):
    model_name: str
    status: Literal["success", "error", "skipped"]
    error_message: str | None = None
    rows_affected: int | None = None
    execution_time_seconds: float = 0.0


class TransformResult(BaseModel):
    models: list[DbtModelOutcome]
    overall_status: Literal["success", "partial", "error"]


class LoadResult(BaseModel):
    rows_loaded: int


class MigrationResult(BaseModel):
    extract: ExtractResult
    transform: TransformResult
    load: LoadResult


def extract_source(run: MigrationRun) -> ExtractResult:
    raise NotImplementedError


def transform(run: MigrationRun) -> TransformResult:
    raise NotImplementedError


def load_target(run: MigrationRun) -> LoadResult:
    raise NotImplementedError


def run_migration(run: MigrationRun) -> MigrationResult:
    raise NotImplementedError
