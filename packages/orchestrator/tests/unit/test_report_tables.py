from pathlib import Path

from aidmi_orchestrator.report.data import load_records
from aidmi_orchestrator.report.tables import (
    appendix_table,
    best_config_table,
    silent_failure_table,
)

FIX = Path(__file__).parent / "fixtures" / "mini_results.jsonl"


def test_best_config_table_returns_html_table():
    recs = load_records([FIX])
    out = best_config_table(recs)
    assert out.startswith("<table")
    assert out.rstrip().endswith("</table>")


def test_best_config_table_picks_plausible_winners():
    recs = load_records([FIX])
    out = best_config_table(recs)
    # highest recall in fixture is 0.75 from write_tools_freeform_inlinedbt/gemini25flash
    assert "write_tools_freeform_inlinedbt" in out
    assert "gemini25flash" in out
    # lowest cost is 0.0 from plan_then_execute/qwen35b (structured_per_table has no cost None case skipped)
    assert "plan_then_execute" in out
    assert "qwen35b" in out


def test_silent_failure_table_returns_html_table():
    recs = load_records([FIX])
    out = silent_failure_table(recs)
    assert out.startswith("<table")
    assert out.rstrip().endswith("</table>")


def test_silent_failure_count_matches_fixture():
    recs = load_records([FIX])
    out = silent_failure_table(recs)
    assert "1 silent failure" in out
    # the single silent-fail row's identifying fields appear in the table
    assert "structured_per_table" in out
    assert "gemini25flash" in out
    assert "master" in out


def test_silent_failure_table_zero_case():
    recs = load_records([FIX])
    non_silent = [r for r in recs if not r.silent_fail]
    out = silent_failure_table(non_silent)
    assert out.startswith("<table")
    assert "0 silent failures" in out


def test_appendix_table_returns_html_table():
    recs = load_records([FIX])
    out = appendix_table(recs)
    assert out.startswith("<table")
    assert out.rstrip().endswith("</table>")


def test_appendix_table_has_coverage_columns():
    recs = load_records([FIX])
    out = appendix_table(recs)
    assert "Tables declared" in out
    assert "Cols covered" in out


def test_appendix_table_has_one_row_per_config():
    recs = load_records([FIX])
    out = appendix_table(recs)
    # 3 fixture records -> 3 distinct (model, cell, ctx, sc) configs in the mini fixture
    assert out.count("<tr") == 1 + 3  # header row + 3 data rows


def test_appendix_table_handles_sc_none():
    recs = load_records([FIX])
    out = appendix_table(recs)
    # plan_write_critique-style records with sc=None aren't in the mini fixture,
    # but structured_per_table row has sc=False -> rendered as "off"
    assert "off" in out


def test_appendix_table_escapes_html_in_dynamic_fields():
    recs = load_records([FIX])
    injected = recs[0].__class__(
        **{**recs[0].__dict__, "cell": "<script>alert(1)</script>"}
    )
    out = appendix_table([injected])
    assert "<script>" not in out
    assert "&lt;script&gt;" in out


def test_appendix_table_sorted_deterministically():
    recs = load_records([FIX])
    out1 = appendix_table(recs)
    out2 = appendix_table(list(reversed(recs)))
    assert out1 == out2
