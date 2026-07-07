from pathlib import Path

from aidmi_orchestrator.report.data import RunRecord, load_records
from aidmi_orchestrator.report.tables import (
    appendix_table,
    best_config_table,
    silent_failure_table,
)

FIX = Path(__file__).parent / "fixtures" / "mini_results.jsonl"


def _mk(cell, ctx, sc, model, *, recall=None, field_acc=None, cost=None,
        materialized=True, rep=0):
    return RunRecord(
        campaign="c", model=model, fixture="f", cell=cell, ctx=ctx, sc=sc, rep=rep,
        materialized=materialized, recall=recall, precision=None, field_acc=field_acc,
        f1=None, recall_strict=None, cost=cost, secs=None, tokens_in=None, tokens_out=None,
        status="complete", silent_fail=False, tables_declared=5, cols_covered=None,
    )


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
    expected = sum(1 for r in recs if r.silent_fail)
    out = silent_failure_table(recs)
    assert f"{expected} silent failure" in out
    # one <tr> per silent-fail row, plus the header row
    assert out.count("<tr") == expected + 1
    # the silent-fail rows' identifying fields appear in the table
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
    expected = len({(r.model, r.cell, r.ctx, r.sc) for r in recs})
    assert out.count("<tr") == 1 + expected  # header row + one row per config


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


def test_sc_none_renders_na_in_best_config_and_appendix():
    recs = [_mk("plan_write_critique", "metadata_only", None, "m", recall=0.9, cost=0.1)]
    bc = best_config_table(recs)
    ap = appendix_table(recs)
    assert "n/a" in bc
    assert "n/a" in ap
    assert "None" not in bc
    assert "None" not in ap


def test_appendix_mean_sd_branch_known_values():
    # recall values 0.4 and 0.8 -> mean 0.600, pstdev 0.200
    recs = [
        _mk("cellA", "metadata_only", True, "m", recall=0.4, field_acc=0.5, rep=0),
        _mk("cellA", "metadata_only", True, "m", recall=0.8, field_acc=0.7, rep=1),
    ]
    out = appendix_table(recs)
    assert "0.600±0.200" in out  # recall mean±pstdev
    assert "0.600±0.100" in out  # field_acc mean 0.6, pstdev 0.1


def test_best_config_deterministic_under_tie():
    # two configs tie at top recall 1.0; winner must not depend on input order
    a = _mk("aaa_cell", "metadata_only", True, "m", recall=1.0)
    b = _mk("zzz_cell", "metadata_only", True, "m", recall=1.0)
    out1 = best_config_table([a, b])
    out2 = best_config_table([b, a])
    assert out1 == out2
