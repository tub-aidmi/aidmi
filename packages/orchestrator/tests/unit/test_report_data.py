from pathlib import Path

from aidmi_orchestrator.report.data import load_records, RunRecord

FIX = Path(__file__).parent / "fixtures" / "mini_results.jsonl"


def test_inline_dbt_cell_disambiguated():
    recs = load_records([FIX])
    cells = {r.cell for r in recs}
    assert "write_tools_freeform_inlinedbt" in cells


def test_model_extracted_from_planner_when_writer_missing():
    recs = load_records([FIX])
    r = next(r for r in recs if r.cell == "plan_then_execute")
    assert r.model == "qwen35b"  # short label mapped from ise-ollama/qwen3.6:35b-a3b


def test_silent_fail_flagged():
    recs = load_records([FIX])
    assert any(r.silent_fail for r in recs)
    sf = next(r for r in recs if r.silent_fail)
    assert sf.status == "complete" and sf.materialized is False


def test_silent_fail_when_dbt_success_but_nothing_materialized():
    recs = load_records([FIX])
    sf = next(
        r for r in recs
        if r.dbt_success and not r.materialized
        and r.status == "complete" and r.recall is None
    )
    assert sf.silent_fail is True


def test_materialized_from_gt_tables_not_dbt_success():
    recs = load_records([FIX])
    partial = next(r for r in recs if r.cell == "structured_per_table" and r.fixture == "master"
                     and r.dbt_success and r.tables_materialized == 0.0)
    assert partial.materialized is False
    assert partial.dbt_success is True


def test_campaign_and_model_always_present():
    recs = load_records([FIX])
    assert all(r.campaign and r.model for r in recs)
