"""run_orchestrator — the 6-step sequential flow."""
from __future__ import annotations
import asyncio
from datetime import datetime
from pathlib import Path
from typing import IO

from aidmi_pipeline.config import MigrationRun, StagingConfig
from aidmi_pipeline.migration import extract_source

from aidmi_orchestrator.api import OrchestratorAPI
from aidmi_orchestrator.discover import discover
from aidmi_orchestrator.domain import TargetSchema
from aidmi_orchestrator.evaluator.base import RunArtifacts, FixtureMetadata
from aidmi_orchestrator.persistence import (
    scaffold_dbt_project, write_strategy_result, write_mapping_manifest,
)
from aidmi_orchestrator.trace import TraceSink, StrategyEvent


class StrategyExecutionError(RuntimeError):
    def __init__(self, run_id: str, inner: Exception):
        super().__init__(f"strategy crashed in run {run_id}: {inner!r}")
        self.run_id = run_id
        self.inner = inner


def _load_target_schema(path: Path | None) -> TargetSchema | None:
    if path is None or not path.exists():
        return None
    return TargetSchema.model_validate_json(path.read_text(encoding="utf-8"))


async def run_orchestrator(
    fixture,
    strategy,
    run_id: str,
    workspace: Path,
    staging_db_url: str,
    trace_mirror: IO[str] | None = None,
) -> RunArtifacts:
    started_at = datetime.utcnow()
    run_dir = workspace / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    dlt_pipelines_dir = run_dir / ".dlt_pipelines"
    dlt_pipelines_dir.mkdir(parents=True, exist_ok=True)
    dbt_project_path = run_dir / "dbt_project"
    scaffold_dbt_project(dbt_project_path)

    trace = TraceSink(run_dir / "trace.jsonl", mirror_to=trace_mirror)
    target_schema = _load_target_schema(fixture.target_schema_path)

    staging = StagingConfig.for_run(staging_db_url, run_id)
    pipeline_run = MigrationRun(
        source=fixture.source_factory(),
        staging=staging,
        target=None,
        target_dataset="",
        target_tables=[],
        dbt_project_path=dbt_project_path,
    )

    # 1. extract
    extract_result = await asyncio.to_thread(extract_source, pipeline_run)
    trace.record(StrategyEvent(
        timestamp=datetime.utcnow(),
        label="extract_complete",
        data={"rows_extracted": extract_result.rows_extracted},
    ))

    # 2. discover
    source_summary = discover(staging.db_url, staging.raw_dataset_name, samples_per_table=100)
    trace.record(StrategyEvent(
        timestamp=datetime.utcnow(),
        label="discover_complete",
        data={"table_count": len(source_summary.tables)},
    ))

    # 3. build api
    api = OrchestratorAPI(
        source_summary=source_summary,
        target_schema=target_schema,
        dbt_project_path=dbt_project_path,
        staging_db_url=staging.db_url,
        staging_raw_dataset=staging.raw_dataset_name,
        staging_out_dataset=staging.out_dataset_name,
        trace=trace,
        _pipeline_run=pipeline_run,
    )

    # 4. run strategy
    try:
        strategy_result = await strategy.generate(api)
    except Exception as e:
        trace.record(StrategyEvent(
            timestamp=datetime.utcnow(),
            label="strategy_crashed",
            data={"error": repr(e), "type": type(e).__name__},
        ))
        trace.close()
        raise StrategyExecutionError(run_id, e) from e

    # 5. final canonical dbt run (best-effort — failures are observed, not raised)
    final_transform = None
    try:
        final_transform = await api.run_dbt()
    except Exception as e:
        trace.record(StrategyEvent(
            timestamp=datetime.utcnow(),
            label="final_dbt_failed",
            data={"error": repr(e), "type": type(e).__name__},
        ))

    # 6. persist
    write_strategy_result(run_dir, strategy_result)
    write_mapping_manifest(run_dir, strategy_result.manifest)
    trace.close()

    wall_clock = (datetime.utcnow() - started_at).total_seconds()
    fixture_meta = FixtureMetadata(
        name=fixture.name,
        description=fixture.description,
        reference_dbt_path=fixture.reference_dbt_path,
        applicable_evaluators=fixture.applicable_evaluators,
    )
    return RunArtifacts(
        run_id=run_id,
        dbt_project_path=dbt_project_path,
        dlt_pipelines_dir=dlt_pipelines_dir,
        staging_db_url=staging.db_url,
        staging_raw_dataset=staging.raw_dataset_name,
        staging_out_dataset=staging.out_dataset_name,
        trace=TraceSink.read_all(run_dir / "trace.jsonl"),
        strategy_result=strategy_result,
        target_schema_input=target_schema,
        fixture=fixture_meta,
        wall_clock_seconds=wall_clock,
        final_transform_result=final_transform,
    )
