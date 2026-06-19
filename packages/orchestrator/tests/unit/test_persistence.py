import json
from datetime import datetime
from aidmi_orchestrator.domain import (
    StrategyResult, MappingManifest, BenchmarkResult,
)
from aidmi_orchestrator.persistence import (
    scaffold_dbt_project,
    write_strategy_result,
    write_mapping_manifest,
    write_benchmark_result,
    archive_run_dbt,
)


def test_scaffold_creates_dbt_project_yml(tmp_path):
    scaffold_dbt_project(tmp_path / "dbt_project")
    assert (tmp_path / "dbt_project" / "dbt_project.yml").exists()
    assert (tmp_path / "dbt_project" / "models").is_dir()


def test_write_strategy_result(tmp_path):
    r = StrategyResult(target_tables_written=["users"], self_reported_status="complete")
    write_strategy_result(tmp_path, r)
    assert json.loads((tmp_path / "strategy_result.json").read_text())["target_tables_written"] == ["users"]


def test_write_mapping_manifest_skips_when_none(tmp_path):
    write_mapping_manifest(tmp_path, None)
    assert not (tmp_path / "mapping_manifest.json").exists()


def test_write_benchmark_result(tmp_path):
    r = BenchmarkResult(
        run_id="r1",
        fixture_name="sp1_users",
        strategy_name="mock",
        strategy_spec_name="mock_spec",
        strategy_config={},
        started_at=datetime(2026, 5, 17),
        completed_at=datetime(2026, 5, 17),
        wall_clock_seconds=0.1,
        strategy_result=StrategyResult(target_tables_written=[], self_reported_status="complete"),
        metrics={"dbt_success": True},
    )
    write_benchmark_result(tmp_path, r)
    data = json.loads((tmp_path / "result.json").read_text())
    assert data["metrics"]["dbt_success"] is True


def test_archive_run_dbt_copies_source(tmp_path):
    run_dir = tmp_path / "run"
    scaffold_dbt_project(run_dir / "dbt_project")
    (run_dir / "dbt_project" / "models" / "users.sql").write_text("SELECT 1", encoding="utf-8")
    (run_dir / "dbt_project" / "target").mkdir()
    (run_dir / "dbt_project" / "target" / "compiled.sql").write_text("big", encoding="utf-8")

    dest = tmp_path / "dest"
    assert archive_run_dbt(run_dir, dest) is True
    assert (dest / "dbt_project" / "dbt_project.yml").exists()
    assert (dest / "dbt_project" / "models" / "users.sql").read_text() == "SELECT 1"
    assert not (dest / "dbt_project" / "target").exists()


def test_archive_run_dbt_idempotent(tmp_path):
    run_dir = tmp_path / "run"
    scaffold_dbt_project(run_dir / "dbt_project")
    dest = tmp_path / "dest"
    assert archive_run_dbt(run_dir, dest) is True
    assert archive_run_dbt(run_dir, dest) is True


def test_archive_run_dbt_missing_source(tmp_path):
    assert archive_run_dbt(tmp_path / "run", tmp_path / "dest") is False
