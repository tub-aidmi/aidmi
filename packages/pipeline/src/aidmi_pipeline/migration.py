from typing import Literal

import dlt
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


def _count_rows_in_dataset(db_url: str, dataset: str) -> int:
    import psycopg2
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name NOT LIKE %s ESCAPE %s",
                (dataset, "\\_dlt%", "\\"),
            )
            tables = [r[0] for r in cur.fetchall()]
            total = 0
            for t in tables:
                cur.execute(f'SELECT COUNT(*) FROM "{dataset}"."{t}"')
                total += cur.fetchone()[0]
            return total


def extract_source(run: MigrationRun) -> ExtractResult:
    pipeline = dlt.pipeline(
        pipeline_name=f"extract_{run.staging.dataset_name}",
        destination=dlt.destinations.postgres(run.staging.db_url),
        dataset_name=run.staging.dataset_name,
    )
    pipeline.run(run.source, write_disposition="replace")
    return ExtractResult(
        rows_extracted=_count_rows_in_dataset(run.staging.db_url, run.staging.dataset_name)
    )


def transform(run: MigrationRun) -> TransformResult:
    raise NotImplementedError


def load_target(run: MigrationRun) -> LoadResult:
    raise NotImplementedError


def run_migration(run: MigrationRun) -> MigrationResult:
    raise NotImplementedError
