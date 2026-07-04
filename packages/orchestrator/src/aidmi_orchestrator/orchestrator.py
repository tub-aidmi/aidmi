"""run_orchestrator — sequential flow."""
from __future__ import annotations
from datetime import datetime
from pathlib import Path
from typing import IO

from aidmi_pipeline.config import MigrationRun, StagingConfig

from aidmi_orchestrator.api import OrchestratorAPI
from aidmi_orchestrator.discover import discover
from aidmi_orchestrator.domain import TargetSchema
from aidmi_orchestrator.evaluator.base import RunArtifacts, FixtureMetadata
from aidmi_orchestrator.persistence import (
    scaffold_dbt_project, write_strategy_result, write_mapping_manifest,
)
from aidmi_orchestrator.progress import log_message
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

    trace = TraceSink(
        run_dir / "trace.jsonl",
        mirror_to=trace_mirror,
        progress_scope=strategy.name,
    )
    target_schema = _load_target_schema(fixture.target_schema_path)

    log_message(f"discovering source schema {fixture.source_schema}", scope=strategy.name)

    staging = StagingConfig.for_run(staging_db_url, fixture.source_schema, run_id)
    pipeline_run = MigrationRun(
        source=None,
        staging=staging,
        target=None,
        target_dataset="",
        target_tables=[],
        dbt_project_path=dbt_project_path,
        fail_fast=False,
    )

    source_summary = discover(staging.db_url, staging.source_schema, samples_per_table=100)
    trace.record(StrategyEvent(
        timestamp=datetime.utcnow(),
        label="discover_complete",
        data={"table_count": len(source_summary.tables)},
    ))
    log_message(
        f"discovered {len(source_summary.tables)} source tables",
        scope=strategy.name,
    )

    api = OrchestratorAPI(
        source_summary=source_summary,
        target_schema=target_schema,
        dbt_project_path=dbt_project_path,
        staging_db_url=staging.db_url,
        source_schema=staging.source_schema,
        out_schema=staging.out_schema,
        trace=trace,
        _pipeline_run=pipeline_run,
    )

    try:
        log_message("starting strategy.generate", scope=strategy.name)
        strategy_result = await strategy.generate(api)
        log_message(
            f"strategy.generate finished ({len(strategy_result.target_tables_written)} tables)",
            scope=strategy.name,
        )
    except Exception as e:
        trace.record(StrategyEvent(
            timestamp=datetime.utcnow(),
            label="strategy_crashed",
            data={"error": repr(e), "type": type(e).__name__},
        ))
        trace.close()
        raise StrategyExecutionError(run_id, e) from e

    final_transform = None
    try:
        log_message("running final dbt transform", scope=strategy.name)
        final_transform = await api.run_dbt()
    except Exception as e:
        trace.record(StrategyEvent(
            timestamp=datetime.utcnow(),
            label="final_dbt_failed",
            data={"error": repr(e), "type": type(e).__name__},
        ))

    write_strategy_result(run_dir, strategy_result)
    write_mapping_manifest(run_dir, strategy_result.manifest)
    trace.close()

    wall_clock = (datetime.utcnow() - started_at).total_seconds()
    fixture_meta = FixtureMetadata(
        name=fixture.name,
        description=fixture.description,
        reference_dbt_path=fixture.reference_dbt_path,
        applicable_evaluators=fixture.applicable_evaluators,
        golden_schema=fixture.golden_schema,
    )
    return RunArtifacts(
        run_id=run_id,
        dbt_project_path=dbt_project_path,
        dlt_pipelines_dir=dlt_pipelines_dir,
        staging_db_url=staging.db_url,
        source_schema=staging.source_schema,
        out_schema=staging.out_schema,
        trace=TraceSink.read_all(run_dir / "trace.jsonl"),
        strategy_result=strategy_result,
        target_schema_input=target_schema,
        fixture=fixture_meta,
        wall_clock_seconds=wall_clock,
        final_transform_result=final_transform,
    )
