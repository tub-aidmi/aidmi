"""Ground-truth evaluators against a minimal golden + produced schema."""
from __future__ import annotations

from pathlib import Path

import psycopg2
import pytest

from aidmi_orchestrator.domain import StrategyResult
from aidmi_orchestrator.evaluator.base import FixtureMetadata, RunArtifacts
from aidmi_orchestrator.evaluator.ground_truth_field_accuracy import (
    GroundTruthFieldAccuracyEvaluator,
)
from aidmi_orchestrator.evaluator.ground_truth_notes import GroundTruthNotesEvaluator
from aidmi_orchestrator.evaluator.ground_truth_recall import GroundTruthRecallEvaluator

GOLDEN = "gt_golden"
OUT = "gt_out"


@pytest.fixture
def seeded_db(staging_db_url):
    with psycopg2.connect(staging_db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{GOLDEN}" CASCADE')
            cur.execute(f'DROP SCHEMA IF EXISTS "{OUT}" CASCADE')
            cur.execute(f'CREATE SCHEMA "{GOLDEN}"')
            cur.execute(f'CREATE SCHEMA "{OUT}"')

            cur.execute(
                f'CREATE TABLE "{GOLDEN}"."Account" ('
                f'"Id" text PRIMARY KEY, "Name" text, "Legacy_Customer_ID__c" text, '
                f'"Customer_Tier__c" text)'
            )
            cur.execute(
                f'CREATE TABLE "{OUT}"."Account" ('
                f'"Id" text PRIMARY KEY, "Name" text, "Legacy_Customer_ID__c" text, '
                f'"Customer_Tier__c" text)'
            )
            cur.execute(
                f'INSERT INTO "{GOLDEN}"."Account" VALUES '
                f"('001A', 'Acme', 'CUST-1', 'Gold'), "
                f"('001B', 'Beta', 'CUST-2', 'Silver')"
            )
            cur.execute(
                f'INSERT INTO "{OUT}"."Account" VALUES '
                f"('001A', 'Acme', 'CUST-1', 'Gold'), "
                f"('001X', 'Wrong', 'CUST-99', 'Bronze')"
            )

            cur.execute(
                f'CREATE TABLE "{GOLDEN}"._ground_truth ('
                f"id serial PRIMARY KEY, target_table text, target_id text, "
                f"source_table text, source_id text, notes text)"
            )
            cur.execute(
                f'INSERT INTO "{GOLDEN}"._ground_truth '
                f"(target_table, target_id, source_table, source_id, notes) VALUES "
                f"('Account', '001A', 'source_account', 'CUST-1', NULL), "
                f"('Account', '001B', 'source_account', 'CUST-2', 'orphan_nulled'), "
                f"('Account', '001A', 'source_account', 'CUST-1_DUP', 'duplicate_account')"
            )
    return staging_db_url


def _artifacts(db_url: str, *, written: list[str] | None = None) -> RunArtifacts:
    return RunArtifacts(
        run_id="r",
        dbt_project_path=Path("/nonexistent"),
        dlt_pipelines_dir=Path("/nonexistent"),
        staging_db_url=db_url,
        source_schema="src_raw",
        out_schema=OUT,
        trace=[],
        strategy_result=StrategyResult(
            target_tables_written=written or ["Account"],
            self_reported_status="complete",
        ),
        target_schema_input=None,
        fixture=FixtureMetadata(
            name="test_v2",
            description="",
            reference_dbt_path=None,
            applicable_evaluators=[],
            golden_schema=GOLDEN,
        ),
        wall_clock_seconds=1.0,
        final_transform_result=None,
    )


def test_recall_evaluator_does_not_apply_without_golden_schema():
    artifacts = _artifacts("postgresql://x")
    artifacts.fixture.golden_schema = None
    assert GroundTruthRecallEvaluator().applies_to(artifacts) is False


def test_recall_evaluator_metrics(seeded_db):
    metrics = GroundTruthRecallEvaluator().evaluate(_artifacts(seeded_db))
    assert metrics["gt_recall_overall"] == pytest.approx(0.5)
    assert metrics["gt_precision_overall"] == pytest.approx(0.5)
    assert metrics["gt_f1_overall"] == pytest.approx(0.5)
    table = metrics["gt_per_table"]["Account"]
    assert table["expected"] == 2
    assert table["produced"] == 2
    assert table["matched"] == 1


def test_notes_evaluator_by_category(seeded_db):
    metrics = GroundTruthNotesEvaluator().evaluate(_artifacts(seeded_db))
    by_cat = metrics["gt_notes_by_category"]
    assert by_cat["clean"]["expected"] == 1
    assert by_cat["clean"]["matched"] == 1
    assert by_cat["clean"]["recall"] == pytest.approx(1.0)
    assert by_cat["orphan_nulled"]["expected"] == 1
    assert by_cat["orphan_nulled"]["matched"] == 0
    assert by_cat["orphan_nulled"]["recall"] == pytest.approx(0.0)
    assert by_cat["duplicate_account"]["expected"] == 1
    assert by_cat["duplicate_account"]["matched"] == 1
    assert by_cat["duplicate_account"]["recall"] == pytest.approx(1.0)


def test_field_accuracy_evaluator(seeded_db):
    metrics = GroundTruthFieldAccuracyEvaluator().evaluate(_artifacts(seeded_db))
    assert metrics["gt_field_accuracy_overall"] == pytest.approx(1.0)
    account = metrics["gt_field_accuracy_per_table"]["Account"]
    assert account["overall"] == pytest.approx(1.0)
    assert account["per_column"]["Name"] == pytest.approx(1.0)
    assert account["per_column"]["Customer_Tier__c"] == pytest.approx(1.0)
