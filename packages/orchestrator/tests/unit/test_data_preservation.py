"""data_preservation: ground-truth-free lossiness signals via SQL aggregates."""
from __future__ import annotations

from pathlib import Path

import psycopg2
import pytest

from aidmi_orchestrator.domain import (
    ColumnNote, MappingManifest, StrategyResult, TableMappingNote,
)
from aidmi_orchestrator.evaluator.base import FixtureMetadata, RunArtifacts
from aidmi_orchestrator.evaluator.data_preservation import DataPreservationEvaluator

RAW, OUT = "dp_raw", "dp_out"


@pytest.fixture
def seeded_db(staging_db_url):
    with psycopg2.connect(staging_db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{RAW}" CASCADE')
            cur.execute(f'DROP SCHEMA IF EXISTS "{OUT}" CASCADE')
            cur.execute(f'CREATE SCHEMA "{RAW}"')
            cur.execute(f'CREATE SCHEMA "{OUT}"')
            cur.execute(f'CREATE TABLE "{RAW}".contacts (id int, email text)')
            cur.execute(
                f"INSERT INTO \"{RAW}\".contacts VALUES "
                f"(1, 'a@x.de'), (2, 'b@x.de'), (3, NULL), (4, NULL)"
            )
            cur.execute(f'CREATE TABLE "{OUT}".users (user_id int, email text)')
            cur.execute(
                f"INSERT INTO \"{OUT}\".users VALUES "
                f"(1, 'a@x.de'), (2, NULL), (3, NULL), (4, NULL)"
            )
            cur.execute(f'CREATE TABLE "{OUT}".empties (x int)')
    return staging_db_url


def _artifacts(db_url: str, manifest: MappingManifest | None, written: list[str]) -> RunArtifacts:
    sr = StrategyResult(target_tables_written=written, manifest=manifest, self_reported_status="complete")
    return RunArtifacts(
        run_id="r", dbt_project_path=Path("/nonexistent"), dlt_pipelines_dir=Path("/nonexistent"),
        staging_db_url=db_url, staging_raw_dataset=RAW, staging_out_dataset=OUT,
        trace=[], strategy_result=sr, target_schema_input=None,
        fixture=FixtureMetadata(name="f", description="", reference_dbt_path=None, applicable_evaluators=[]),
        wall_clock_seconds=1.0, final_transform_result=None,
    )


def _manifest(source_col: str = "contacts.email") -> MappingManifest:
    return MappingManifest(strategy_name="x", strategy_config={}, tables=[
        TableMappingNote(target_table="users", source_tables=["contacts"], column_notes=[
            ColumnNote(target_column="email", source_columns=[source_col]),
        ]),
    ])


def test_manifest_mapped_mode_metrics(seeded_db) -> None:
    metrics = DataPreservationEvaluator().evaluate(_artifacts(seeded_db, _manifest(), ["users", "empties"]))
    assert metrics["preservation_mode"] == "manifest_mapped"
    assert metrics["preservation_empty_tables"] == 1
    # users: 4 rows out / 4 source rows
    assert metrics["preservation_per_table"]["users"]["row_ratio"] == 1.0
    # email null rate: out 0.75, source 0.5 -> inflation 0.25
    assert metrics["preservation_null_inflation_mean"] == pytest.approx(0.25)
    assert metrics["preservation_null_inflation_max"] == pytest.approx(0.25)
    assert metrics["preservation_high_null_inflation_columns"] == 1
    # email distinct: out 1 / source 2 -> 0.5
    assert metrics["preservation_distinct_ratio_mean"] == pytest.approx(0.5)
    assert metrics["preservation_unresolved_mappings"] == 0


def test_bare_column_name_resolves_when_unique(seeded_db) -> None:
    metrics = DataPreservationEvaluator().evaluate(_artifacts(seeded_db, _manifest("email"), ["users"]))
    assert metrics["preservation_unresolved_mappings"] == 0
    assert metrics["preservation_null_inflation_mean"] == pytest.approx(0.25)


def test_unresolvable_mapping_is_counted(seeded_db) -> None:
    metrics = DataPreservationEvaluator().evaluate(_artifacts(seeded_db, _manifest("ghost.col"), ["users"]))
    assert metrics["preservation_unresolved_mappings"] == 1
    assert metrics["preservation_null_inflation_mean"] is None


def test_aggregate_only_mode_without_manifest(seeded_db) -> None:
    metrics = DataPreservationEvaluator().evaluate(_artifacts(seeded_db, None, ["users"]))
    assert metrics["preservation_mode"] == "aggregate_only"
    assert metrics["preservation_per_table"]["users"]["row_ratio"] == 1.0
    assert metrics["preservation_null_inflation_mean"] is None


def test_applies_to_requires_written_tables(seeded_db) -> None:
    ev = DataPreservationEvaluator()
    assert ev.applies_to(_artifacts(seeded_db, None, ["users"])) is True
    assert ev.applies_to(_artifacts(seeded_db, None, [])) is False
