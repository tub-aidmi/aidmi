"""Ground-truth evaluators against a minimal golden + produced schema."""

from __future__ import annotations

from pathlib import Path

import psycopg2
import pytest
from aidmi_orchestrator.domain import StrategyResult
from aidmi_orchestrator.evaluator._ground_truth_utils import ground_truth_row_matched
from aidmi_orchestrator.evaluator.base import FixtureMetadata, RunArtifacts
from aidmi_orchestrator.evaluator.ground_truth_field_accuracy import (
    GroundTruthFieldAccuracyEvaluator,
)
from aidmi_orchestrator.evaluator.ground_truth_fk_integrity import (
    GroundTruthFkIntegrityEvaluator,
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
                f"('Account', 'CUST-1', 'source_account', 'CUST-1_DUP', 'duplicate_account'), "
                f"('Account', 'CUST-2', 'source_account', 'CUST-2_DUP', 'duplicate_account')"
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


def _dup_gt(survivor_legacy: str, dup_legacy: str) -> dict[str, str]:
    return {
        "target_table": "Account",
        "target_id": survivor_legacy,
        "source_table": "source_account",
        "source_id": dup_legacy,
        "notes": "duplicate_account",
    }


def test_duplicate_matched_when_folded_into_survivor():
    """Correct dedup: survivor legacy present, duplicate legacy folded away."""
    produced_by_legacy = {"CUST-1": {"Id": "x"}}
    assert ground_truth_row_matched(
        _dup_gt("CUST-1", "CUST-1_DUP"),
        produced_by_legacy=produced_by_legacy,
        produced_by_id={},
    )


def test_duplicate_not_matched_when_duplicate_row_survives():
    """Failure to dedup: the duplicate's own legacy id still emitted as a row."""
    produced_by_legacy = {"CUST-1": {"Id": "x"}, "CUST-1_DUP": {"Id": "y"}}
    assert not ground_truth_row_matched(
        _dup_gt("CUST-1", "CUST-1_DUP"),
        produced_by_legacy=produced_by_legacy,
        produced_by_id={},
    )


def test_duplicate_not_matched_when_survivor_absent():
    """Dropping the whole identity is not a correct merge."""
    produced_by_legacy = {"CUST-99": {"Id": "x"}}
    assert not ground_truth_row_matched(
        _dup_gt("CUST-1", "CUST-1_DUP"),
        produced_by_legacy=produced_by_legacy,
        produced_by_id={},
    )


def test_normal_row_matches_on_legacy_id():
    gt = {
        "target_table": "Account",
        "target_id": "001A",
        "source_table": "source_account",
        "source_id": "CUST-1",
        "notes": None,
    }
    assert ground_truth_row_matched(
        gt, produced_by_legacy={"CUST-1": {}}, produced_by_id={}
    )
    assert not ground_truth_row_matched(
        gt, produced_by_legacy={"CUST-9": {}}, produced_by_id={}
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
    # CUST-1 folded (survivor present, dup absent) -> matched; CUST-2 survivor
    # never produced -> not matched.
    assert by_cat["duplicate_account"]["expected"] == 2
    assert by_cat["duplicate_account"]["matched"] == 1
    assert by_cat["duplicate_account"]["recall"] == pytest.approx(0.5)


def test_field_accuracy_evaluator(seeded_db):
    metrics = GroundTruthFieldAccuracyEvaluator().evaluate(_artifacts(seeded_db))
    assert metrics["gt_field_accuracy_overall"] == pytest.approx(1.0)
    account = metrics["gt_field_accuracy_per_table"]["Account"]
    assert account["overall"] == pytest.approx(1.0)
    assert account["per_column"]["Name"] == pytest.approx(1.0)
    assert account["per_column"]["Customer_Tier__c"] == pytest.approx(1.0)


def test_recall_evaluator_missing_table_lowers_overall_recall(seeded_db):
    """A declared-but-missing table must count in the recall denominator."""
    with psycopg2.connect(seeded_db) as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE "{OUT}"."Account"')

    metrics = GroundTruthRecallEvaluator().evaluate(_artifacts(seeded_db))
    assert metrics["gt_per_table"]["Account"]["missing_table"] is True
    assert metrics["gt_per_table"]["Account"]["expected"] == 2
    assert metrics["gt_per_table"]["Account"]["matched"] == 0
    assert metrics["gt_recall_overall"] == pytest.approx(0.0)
    assert metrics["gt_tables_materialized"] == pytest.approx(0.0)


@pytest.fixture
def fk_db(staging_db_url):
    """Account + Contact with a remapped FK id space for FK-integrity tests."""
    with psycopg2.connect(staging_db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{GOLDEN}" CASCADE')
            cur.execute(f'DROP SCHEMA IF EXISTS "{OUT}" CASCADE')
            cur.execute(f'CREATE SCHEMA "{GOLDEN}"')
            cur.execute(f'CREATE SCHEMA "{OUT}"')

            for schema in (GOLDEN, OUT):
                cur.execute(
                    f'CREATE TABLE "{schema}"."Account" ('
                    f'"Id" text PRIMARY KEY, "Legacy_Customer_ID__c" text)'
                )
                cur.execute(
                    f'CREATE TABLE "{schema}"."Contact" ('
                    f'"Id" text PRIMARY KEY, "FirstName" text, '
                    f'"Legacy_Contact_ID__c" text, "AccountId" text)'
                )

            cur.execute(
                f'INSERT INTO "{GOLDEN}"."Account" VALUES '
                f"('001A', 'CUST-1'), ('001B', 'CUST-2')"
            )
            # 'PA1' is a remapped surrogate for CUST-1: same legacy account,
            # different Id than golden's '001A'. FK resolution must see through it.
            cur.execute(
                f'INSERT INTO "{OUT}"."Account" VALUES '
                f"('PA1', 'CUST-1'), ('001X', 'CUST-99')"
            )

            # golden AccountId -> CUST-1, CUST-2, orphan(NULL), CUST-1
            cur.execute(
                f'INSERT INTO "{GOLDEN}"."Contact" VALUES '
                f"('C1g', 'Ann', 'CT-1', '001A'), "
                f"('C2g', 'Bob', 'CT-2', '001B'), "
                f"('C3g', 'Cy', 'CT-3', NULL), "
                f"('C4g', 'Di', 'CT-4', '001A')"
            )
            # produced: CT-1 match (remapped PA1->CUST-1), CT-2 wrong parent,
            # CT-3 orphan preserved (match), CT-4 dangling ref (miss)
            cur.execute(
                f'INSERT INTO "{OUT}"."Contact" VALUES '
                f"('C1p', 'Ann', 'CT-1', 'PA1'), "
                f"('C2p', 'Bob', 'CT-2', 'PA1'), "
                f"('C3p', 'Cy', 'CT-3', NULL), "
                f"('C4p', 'Di', 'CT-4', '999ZZ')"
            )
    return staging_db_url


def test_fk_integrity_evaluator(fk_db):
    artifacts = _artifacts(fk_db, written=["Account", "Contact"])
    metrics = GroundTruthFkIntegrityEvaluator().evaluate(artifacts)

    # CT-1 remapped-match, CT-3 orphan-match; CT-2 wrong parent, CT-4 dangling.
    assert metrics["gt_fk_integrity_overall"] == pytest.approx(0.5)
    assert metrics["gt_fk_dangling_total"] == 1

    contact = metrics["gt_fk_integrity_per_table"]["Contact"]
    assert contact["compared_cells"] == 4
    assert contact["matched_cells"] == 2
    assert contact["per_column"]["AccountId"] == pytest.approx(0.5)
    assert contact["dangling_per_column"]["AccountId"] == 1


def test_fk_integrity_does_not_apply_without_golden_schema():
    artifacts = _artifacts("postgresql://x")
    artifacts.fixture.golden_schema = None
    assert GroundTruthFkIntegrityEvaluator().applies_to(artifacts) is False
