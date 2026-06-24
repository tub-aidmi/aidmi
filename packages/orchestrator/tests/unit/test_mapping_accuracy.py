"""mapping_accuracy: recall-oriented scoring against _field_mapping ground truth."""
from __future__ import annotations

import json
from pathlib import Path

from aidmi_orchestrator.domain import (
    ColumnNote, MappingManifest, StrategyResult, TableMappingNote,
)
from aidmi_orchestrator.evaluator.base import FixtureMetadata, RunArtifacts
from aidmi_orchestrator.evaluator.mapping_accuracy import MappingAccuracyEvaluator


GT = {
    "case": "demo",
    "edges": [
        {"source_table": "kunden", "source_column": "nr", "target_table": "Account",
         "target_column": "Legacy_Customer_ID__c", "transform": None, "notes": "External ID"},
        {"source_table": "kunden", "source_column": "name", "target_table": "Account",
         "target_column": "Name", "transform": None, "notes": None},
        {"source_table": "kontakte", "source_column": "account_ref", "target_table": "Contact",
         "target_column": "AccountId", "transform": "lookup", "notes": None},
        {"source_table": "kontakte", "source_column": "company_name", "target_table": "Contact",
         "target_column": "AccountId", "transform": "lookup", "notes": None},
    ],
}


def _gt_file(tmp_path: Path) -> Path:
    p = tmp_path / "ground_truth.json"
    p.write_text(json.dumps(GT), encoding="utf-8")
    return p


def _artifacts(manifest, gt_path) -> RunArtifacts:
    sr = StrategyResult(
        target_tables_written=["Account", "Contact"],
        manifest=manifest, self_reported_status="complete",
    )
    return RunArtifacts(
        run_id="r", dbt_project_path=Path("/nonexistent"), dlt_pipelines_dir=Path("/nonexistent"),
        staging_db_url="postgresql://", staging_raw_dataset="raw", staging_out_dataset="out",
        trace=[], strategy_result=sr, target_schema_input=None,
        fixture=FixtureMetadata(name="f", description="", reference_dbt_path=None,
                                applicable_evaluators=[], ground_truth_mapping_path=gt_path),
        wall_clock_seconds=1.0, final_transform_result=None,
    )


def _manifest(tables) -> MappingManifest:
    return MappingManifest(strategy_name="x", strategy_config={}, tables=tables)


def test_applies_with_gt_regardless_of_manifest(tmp_path):
    ev = MappingAccuracyEvaluator()
    gt = _gt_file(tmp_path)
    assert ev.applies_to(_artifacts(_manifest([]), gt)) is True
    assert ev.applies_to(_artifacts(None, gt)) is True
    no_gt = _artifacts(_manifest([]), gt)
    no_gt.fixture.ground_truth_mapping_path = None
    assert ev.applies_to(no_gt) is False


def test_perfect_recall(tmp_path):
    manifest = _manifest([
        TableMappingNote(target_table="Account", column_notes=[
            ColumnNote(target_column="Legacy_Customer_ID__c", source_columns=["kunden.nr"]),
            ColumnNote(target_column="Name", source_columns=["kunden.name"]),
        ]),
        TableMappingNote(target_table="Contact", column_notes=[
            ColumnNote(target_column="AccountId", source_columns=["kontakte.account_ref", "kontakte.company_name"]),
        ]),
    ])
    m = MappingAccuracyEvaluator().evaluate(_artifacts(manifest, _gt_file(tmp_path)))
    assert m["ground_truth_edges"] == 4
    assert m["edge_recall"] == 1.0
    assert m["column_recall"] == 1.0
    assert m["conflicting_edges"] == 0
    assert m["unmapped_ground_truth"] == []


def test_column_recall_credits_single_fallback(tmp_path):
    manifest = _manifest([
        TableMappingNote(target_table="Account", column_notes=[
            ColumnNote(target_column="Legacy_Customer_ID__c", source_columns=["nr"]),
            ColumnNote(target_column="Name", source_columns=["name"]),
        ]),
        TableMappingNote(target_table="Contact", column_notes=[
            ColumnNote(target_column="AccountId", source_columns=["account_ref"]),
        ]),
    ])
    m = MappingAccuracyEvaluator().evaluate(_artifacts(manifest, _gt_file(tmp_path)))
    assert m["edge_recall"] == 0.75
    assert m["column_recall"] == 1.0
    assert m["conflicting_edges"] == 0


def test_conflicting_edge_flagged(tmp_path):
    manifest = _manifest([
        TableMappingNote(target_table="Account", column_notes=[
            ColumnNote(target_column="Legacy_Customer_ID__c", source_columns=["kunden.nr"]),
            ColumnNote(target_column="Name", source_columns=["nr"]),
        ]),
        TableMappingNote(target_table="Contact", column_notes=[
            ColumnNote(target_column="AccountId", source_columns=["kontakte.account_ref"]),
        ]),
    ])
    m = MappingAccuracyEvaluator().evaluate(_artifacts(manifest, _gt_file(tmp_path)))
    assert m["conflicting_edges"] == 1
    assert m["unmapped_ground_truth"] == ["Account.Name"]


def test_extra_edges_not_penalised(tmp_path):
    manifest = _manifest([
        TableMappingNote(target_table="Account", column_notes=[
            ColumnNote(target_column="Legacy_Customer_ID__c", source_columns=["nr"]),
            ColumnNote(target_column="Name", source_columns=["name"]),
            ColumnNote(target_column="Industry", source_columns=["branche"]),
        ]),
        TableMappingNote(target_table="Contact", column_notes=[
            ColumnNote(target_column="AccountId", source_columns=["account_ref", "company_name"]),
        ]),
    ])
    m = MappingAccuracyEvaluator().evaluate(_artifacts(manifest, _gt_file(tmp_path)))
    assert m["edge_recall"] == 1.0
    assert m["conflicting_edges"] == 0


def test_no_manifest_reports_present_false(tmp_path):
    a = _artifacts(None, _gt_file(tmp_path))
    m = MappingAccuracyEvaluator().evaluate(a)
    assert m["manifest_present"] is False
    assert m["ground_truth_transform_edges"] is None
    assert m["edge_recall"] is None
    assert m["column_recall"] is None


def test_camelcase_identifiers_reconcile_with_dlt_normalization(tmp_path):
    gt = {
        "case": "messy",
        "edges": [
            {"source_table": "Opportunity", "source_column": "StageName", "target_table": "Opportunity",
             "target_column": "StageName", "transform": "map_stage", "notes": None},
            {"source_table": "Opportunity", "source_column": "CloseDate", "target_table": "Opportunity",
             "target_column": "CloseDate", "transform": None, "notes": None},
        ],
    }
    p = tmp_path / "ground_truth.json"
    p.write_text(json.dumps(gt), encoding="utf-8")
    manifest = _manifest([
        TableMappingNote(target_table="Opportunity", column_notes=[
            ColumnNote(target_column="StageName", source_columns=["opportunity.stage_name"]),
            ColumnNote(target_column="CloseDate", source_columns=["close_date"]),
        ]),
    ])
    m = MappingAccuracyEvaluator().evaluate(_artifacts(manifest, p))
    assert m["edge_recall"] == 1.0
    assert m["column_recall"] == 1.0
    assert m["conflicting_edges"] == 0
    assert m["unmapped_ground_truth"] == []
