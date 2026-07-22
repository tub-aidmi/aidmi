"""Re-apply archived dbt and re-evaluate recorded runs."""

from __future__ import annotations

import asyncio
from pathlib import Path

from aidmi_orchestrator.campaign import (
    Campaign,
    resolve_dbt_project,
    resolve_run_bundle,
)
from aidmi_orchestrator.domain import BenchmarkResult
from aidmi_orchestrator.evaluator.base import (
    FixtureMetadata,
    RunArtifacts,
    make_evaluator,
)
from aidmi_orchestrator.fixtures.base import get_fixture
from aidmi_orchestrator.persistence import load_result_from_campaign
from aidmi_orchestrator.trace import TraceSink
from aidmi_pipeline.config import MigrationRun, StagingConfig
from aidmi_pipeline.migration import transform


def apply_dbt_to_postgres(
    *,
    dbt_project_path: Path,
    staging_db_url: str,
    source_schema: str,
    out_schema: str,
):
    run = MigrationRun(
        source=None,
        staging=StagingConfig(
            db_url=staging_db_url,
            source_schema=source_schema,
            out_schema=out_schema,
        ),
        target=None,
        target_dataset="",
        target_tables=[],
        dbt_project_path=dbt_project_path,
    )
    return transform(run)


def apply_recorded_run(
    campaign: Campaign,
    run_id: str,
    staging_db_url: str,
    *,
    rep_index: int = 0,
):
    result = load_result_from_campaign(campaign.path, run_id, rep_index)
    dbt_project = resolve_dbt_project(campaign.path, run_id, rep_index)
    transform_result = apply_dbt_to_postgres(
        dbt_project_path=dbt_project,
        staging_db_url=staging_db_url,
        source_schema=result.source_schema,
        out_schema=result.out_schema,
    )
    return result, transform_result


def _target_schema_from_result(result: BenchmarkResult):
    return result.strategy_result.target_schema


def build_artifacts_for_evaluate(
    campaign: Campaign,
    result: BenchmarkResult,
    staging_db_url: str,
    *,
    rep_index: int = 0,
    transform_result=None,
) -> RunArtifacts:
    bundle = resolve_run_bundle(campaign.path, result.run_id, rep_index)
    fixture = get_fixture(result.fixture_name)
    trace_path = bundle / "trace.jsonl"
    trace = TraceSink.read_all(trace_path) if trace_path.is_file() else []
    return RunArtifacts(
        run_id=result.run_id,
        dbt_project_path=bundle / "dbt_project",
        dlt_pipelines_dir=bundle / ".dlt_pipelines",
        staging_db_url=staging_db_url,
        source_schema=result.source_schema,
        out_schema=result.out_schema,
        trace=trace,
        strategy_result=result.strategy_result,
        target_schema_input=_target_schema_from_result(result),
        fixture=FixtureMetadata(
            name=fixture.name,
            description=fixture.description,
            reference_dbt_path=fixture.reference_dbt_path,
            applicable_evaluators=fixture.applicable_evaluators,
            golden_schema=fixture.golden_schema,
        ),
        wall_clock_seconds=result.wall_clock_seconds,
        final_transform_result=transform_result,
    )


async def evaluate_recorded_run(
    campaign: Campaign,
    run_id: str,
    staging_db_url: str,
    *,
    rep_index: int = 0,
    transform_result=None,
) -> dict:
    result = load_result_from_campaign(campaign.path, run_id, rep_index)
    artifacts = build_artifacts_for_evaluate(
        campaign,
        result,
        staging_db_url,
        rep_index=rep_index,
        transform_result=transform_result,
    )
    metrics: dict = {}
    for ev in [
        make_evaluator(name) for name in fixture_evaluators(result.fixture_name)
    ]:
        if ev.applies_to(artifacts):
            part = await asyncio.to_thread(ev.evaluate, artifacts)
            metrics.update(part)
    return metrics


def fixture_evaluators(fixture_name: str) -> list[str]:
    return get_fixture(fixture_name).applicable_evaluators
