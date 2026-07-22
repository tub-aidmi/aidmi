"""manifest_quality: structural scoring of the explanation artifact."""

from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.domain import (
    ColumnNote,
    MappingManifest,
    StrategyResult,
    TableMappingNote,
    TargetColumn,
    TargetSchema,
    TargetTable,
)
from aidmi_orchestrator.evaluator.base import FixtureMetadata, RunArtifacts
from aidmi_orchestrator.evaluator.manifest_quality import ManifestQualityEvaluator


def _artifacts(
    strategy_result: StrategyResult, target: TargetSchema | None
) -> RunArtifacts:
    return RunArtifacts(
        run_id="r",
        dbt_project_path=Path("/nonexistent"),
        dlt_pipelines_dir=Path("/nonexistent"),
        staging_db_url="postgresql://",
        source_schema="raw",
        out_schema="out",
        trace=[],
        strategy_result=strategy_result,
        target_schema_input=target,
        fixture=FixtureMetadata(
            name="f", description="", reference_dbt_path=None, applicable_evaluators=[]
        ),
        wall_clock_seconds=1.0,
        final_transform_result=None,
    )


def _target() -> TargetSchema:
    return TargetSchema(
        tables=[
            TargetTable(
                name="users",
                columns=[
                    TargetColumn(name="user_id", sql_type="integer"),
                    TargetColumn(name="email", sql_type="text", nullable=True),
                ],
            )
        ]
    )


def test_no_manifest_scores_present_false() -> None:
    sr = StrategyResult(
        target_tables_written=["users"], manifest=None, self_reported_status="complete"
    )
    metrics = ManifestQualityEvaluator().evaluate(_artifacts(sr, _target()))
    assert metrics["manifest_present"] is False
    assert metrics["manifest_table_coverage"] == 0.0
    assert metrics["manifest_column_coverage"] == 0.0


def test_full_manifest_scores_full_coverage() -> None:
    manifest = MappingManifest(
        strategy_name="x",
        strategy_config={},
        tables=[
            TableMappingNote(
                target_table="users",
                source_tables=["contacts"],
                reasoning="because",
                column_notes=[
                    ColumnNote(
                        target_column="user_id",
                        source_columns=["contacts.id"],
                        explanation="direct",
                    ),
                    ColumnNote(
                        target_column="email",
                        source_columns=["contacts.email"],
                        explanation="lowercased",
                    ),
                ],
            ),
        ],
    )
    sr = StrategyResult(
        target_tables_written=["users"],
        manifest=manifest,
        self_reported_status="complete",
    )
    metrics = ManifestQualityEvaluator().evaluate(_artifacts(sr, _target()))
    assert metrics["manifest_present"] is True
    assert metrics["manifest_table_coverage"] == 1.0
    assert metrics["manifest_column_coverage"] == 1.0
    assert metrics["manifest_explanation_rate"] == 1.0
    assert metrics["manifest_reasoning_rate"] == 1.0


def test_partial_manifest_scores_fractions() -> None:
    manifest = MappingManifest(
        strategy_name="x",
        strategy_config={},
        tables=[
            TableMappingNote(
                target_table="users",
                source_tables=[],
                reasoning="",
                column_notes=[
                    ColumnNote(
                        target_column="user_id",
                        source_columns=["contacts.id"],
                        explanation="",
                    ),
                ],
            ),
        ],
    )
    sr = StrategyResult(
        target_tables_written=["users", "orgs"],
        manifest=manifest,
        self_reported_status="partial",
    )
    metrics = ManifestQualityEvaluator().evaluate(_artifacts(sr, _target()))
    assert metrics["manifest_table_coverage"] == 0.5  # users noted, orgs not
    assert metrics["manifest_column_coverage"] == 0.5  # user_id noted, email not
    assert metrics["manifest_explanation_rate"] == 0.0
    assert metrics["manifest_reasoning_rate"] == 0.0


def test_no_target_schema_yields_none_column_coverage() -> None:
    sr = StrategyResult(
        target_tables_written=["users"], manifest=None, self_reported_status="complete"
    )
    metrics = ManifestQualityEvaluator().evaluate(_artifacts(sr, None))
    assert metrics["manifest_column_coverage"] is None


def test_applies_to_everything() -> None:
    sr = StrategyResult(
        target_tables_written=[], manifest=None, self_reported_status="gave_up"
    )
    assert ManifestQualityEvaluator().applies_to(_artifacts(sr, None)) is True
