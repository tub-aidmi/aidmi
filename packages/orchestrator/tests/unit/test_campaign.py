import json
from datetime import datetime
from pathlib import Path

import yaml

from aidmi_orchestrator.campaign import (
    Campaign,
    bundle_dir_for_run,
    make_campaign_id,
    resolve_run_bundle,
    results_jsonl_for_campaign,
)
from aidmi_orchestrator.domain import BenchmarkResult, StrategyResult
from aidmi_orchestrator.persistence import record_run, scaffold_dbt_project


def _result(
    run_id: str = "rabc1234_mock_master", rep_index: int = 0
) -> BenchmarkResult:
    now = datetime.utcnow()
    return BenchmarkResult(
        run_id=run_id,
        fixture_name="master",
        strategy_name="mock",
        strategy_spec_name="mock",
        strategy_config={},
        rep_index=rep_index,
        started_at=now,
        completed_at=now,
        wall_clock_seconds=1.0,
        strategy_result=StrategyResult(
            target_tables_written=[], self_reported_status="complete"
        ),
        metrics={"dbt_success": True},
        source_schema="fixture_master_src",
        out_schema="rabc1234_mock_master",
    )


def test_make_campaign_id_format():
    cid = make_campaign_id()
    assert len(cid.split("-")) >= 4
    date_part = "-".join(cid.split("-")[:3])
    assert len(date_part) == 10


def test_campaign_create(tmp_path):
    root = tmp_path / "benchmarks"
    camp = Campaign.create(label="test run", root=root)
    assert camp.path.is_dir()
    assert (camp.path / "runs").is_dir()
    meta = yaml.safe_load(camp.campaign_yaml.read_text())
    assert meta["label"] == "test run"
    assert meta["id"] == camp.id


def test_record_run_writes_bundle_and_jsonl(tmp_path):
    camp = Campaign.create(root=tmp_path)
    workspace = tmp_path / "workspace"
    run_id = "rabc1234_mock_master"
    ws_run = workspace / "runs" / run_id
    ws_run.mkdir(parents=True)
    scaffold_dbt_project(ws_run / "dbt_project")
    (ws_run / "dbt_project" / "models" / "t.sql").write_text(
        "SELECT 1", encoding="utf-8"
    )
    (ws_run / "trace.jsonl").write_text('{"event_type":"strategy"}\n', encoding="utf-8")
    spec = tmp_path / "spec.yaml"
    spec.write_text("name: mock\nstrategy: mock\n", encoding="utf-8")

    result = _result(run_id)
    bundle = record_run(
        camp.path,
        result,
        ws_run,
        strategy_spec_path=spec,
    )
    assert bundle.name == run_id
    assert (bundle / "result.json").is_file()
    assert (bundle / "strategy_spec.yaml").is_file()
    assert (bundle / "dbt_project" / "models" / "t.sql").is_file()
    lines = camp.results_jsonl.read_text().strip().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["run_id"] == run_id


def test_bundle_dir_rep_suffix():
    assert bundle_dir_for_run(Path("/c"), "r1", 0).name == "r1"
    assert bundle_dir_for_run(Path("/c"), "r1", 2).name == "r1_rep2"


def test_results_jsonl_legacy_layout(tmp_path):
    legacy = tmp_path / "old"
    (legacy / "results").mkdir(parents=True)
    (legacy / "results" / "results.jsonl").write_text("{}\n", encoding="utf-8")
    assert results_jsonl_for_campaign(legacy) == legacy / "results" / "results.jsonl"


def test_resolve_run_bundle_legacy_dbt(tmp_path):
    camp = tmp_path / "camp"
    run_id = "rlegacy1"
    dbt = camp / "results" / "dbt" / run_id / "dbt_project"
    dbt.mkdir(parents=True)
    (dbt / "dbt_project.yml").write_text("x", encoding="utf-8")
    resolved = resolve_run_bundle(camp, run_id)
    assert resolved == camp / "results" / "dbt" / run_id
