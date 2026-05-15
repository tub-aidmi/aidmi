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


def _outcome_to_model(outcome) -> DbtModelOutcome:
    raw_status = getattr(outcome, "status", "error")
    status: Literal["success", "error", "skipped"] = (
        raw_status if raw_status in {"success", "error", "skipped"} else "error"
    )
    return DbtModelOutcome(
        model_name=getattr(outcome, "model_name", "<unknown>"),
        status=status,
        error_message=getattr(outcome, "message", None) if status != "success" else None,
        rows_affected=None,
        execution_time_seconds=float(getattr(outcome, "time", 0.0) or 0.0),
    )


def _overall_status(models: list[DbtModelOutcome]) -> Literal["success", "partial", "error"]:
    statuses = {m.status for m in models}
    if statuses == {"success"} or not statuses:
        return "success"
    if "success" in statuses:
        return "partial"
    return "error"


def transform(run: MigrationRun) -> TransformResult:
    pipeline = dlt.pipeline(
        pipeline_name=f"dbt_{run.staging.dataset_name}",
        destination=dlt.destinations.postgres(run.staging.db_url),
        dataset_name=run.staging.dataset_name,
    )
    venv = dlt.dbt.get_venv(pipeline)
    runner = dlt.dbt.package(pipeline, str(run.dbt_project_path), venv=venv)
    outcomes = runner.run_all()
    models = [_outcome_to_model(o) for o in outcomes]
    return TransformResult(models=models, overall_status=_overall_status(models))


def _count_table_rows(db_url: str, dataset: str, table: str) -> int:
    import psycopg2
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM "{dataset}"."{table}"')
                return cur.fetchone()[0]
    except psycopg2.errors.UndefinedTable as e:
        raise ValueError(
            f"table {dataset}.{table} not found in staging — "
            f"was it produced by the transform phase?"
        ) from e


def load_target(run: MigrationRun) -> LoadResult:
    from dlt.sources.sql_database import sql_table

    pipeline = dlt.pipeline(
        pipeline_name=f"load_{run.target_dataset}",
        destination=run.target,
        dataset_name=run.target_dataset,
    )
    total_rows = 0
    for table in run.target_tables:
        rows_in_staging = _count_table_rows(
            run.staging.db_url, run.staging.dataset_name, table
        )
        pipeline.run(
            sql_table(
                credentials=run.staging.db_url,
                schema=run.staging.dataset_name,
                table=table,
            ),
            write_disposition="replace",
            loader_file_format="jsonl",
        )
        total_rows += rows_in_staging
    return LoadResult(rows_loaded=total_rows)


def run_migration(run: MigrationRun) -> MigrationResult:
    return MigrationResult(
        extract=extract_source(run),
        transform=transform(run),
        load=load_target(run),
    )
