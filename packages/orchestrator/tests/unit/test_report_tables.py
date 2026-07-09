from pathlib import Path

from aidmi_orchestrator.report.data import RunRecord, load_records
from aidmi_orchestrator.report.tables import (
    appendix_table,
    best_config_table,
    silent_failure_table,
    summary_best_config_table,
    summary_by_ctx_table,
    summary_by_sc_table,
    summary_overall_table,
    summary_sc_block,
)

FIX = Path(__file__).parent / "fixtures" / "mini_results.jsonl"


def _mk(cell, ctx, sc, model, *, recall=None, field_acc=None, cost=None,
        materialized=True, rep=0, fixture="f"):
    return RunRecord(
        campaign="c", model=model, fixture=fixture, cell=cell, ctx=ctx, sc=sc, rep=rep,
        dbt_success=materialized, materialized=materialized,
        tables_materialized=1.0 if materialized else 0.0,
        recall=recall, precision=None, field_acc=field_acc,
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


def test_summary_best_config_has_all_four_objectives():
    recs = [
        _mk("a", "metadata_only", True, "m", recall=1.0, field_acc=0.5, cost=0.9),
        _mk("b", "live_query_tool", False, "m", recall=0.3, field_acc=0.9, cost=0.1),
    ]
    out = summary_best_config_table(recs)
    assert "Highest mean recall" in out
    assert "Highest mean field acc" in out
    assert "Lowest mean cost" in out
    assert "Highest full-materialization rate" in out
    assert "Objective" in out and "Winning config" in out
    # no separate value column
    assert "Value" not in out
    # header + four objective rows
    assert out.count("<tr") == 5


def test_summary_best_config_picks_distinct_winners():
    recs = [
        _mk("recall_king", "metadata_only", True, "m", recall=1.0, field_acc=0.4, cost=0.9),
        _mk("field_king", "live_query_tool", False, "m", recall=0.3, field_acc=0.95, cost=0.5),
        _mk("cheap", "metadata_only", True, "m", recall=0.2, field_acc=0.2, cost=0.01),
    ]
    out = summary_best_config_table(recs)
    assert "recall_king / metadata_only / sc on" in out
    assert "field_king / live_query_tool / sc off" in out
    assert "cheap / metadata_only / sc on" in out


def test_summary_best_config_materialization_uses_all_not_any():
    # both configs materialize at least one table (mat-any ties at 100%),
    # but only "full" materializes every table -> it must win.
    full = _mk("full", "metadata_only", True, "m", recall=0.1, materialized=True)
    partial = RunRecord(
        campaign="c", model="m", fixture="f", cell="partial", ctx="metadata_only",
        sc=True, rep=0, dbt_success=True, materialized=True,
        tables_materialized=0.5, recall=0.9, precision=None, field_acc=None,
        f1=None, recall_strict=None, cost=None, secs=None, tokens_in=None,
        tokens_out=None, status="complete", silent_fail=False,
        tables_declared=5, cols_covered=None,
    )
    out = summary_best_config_table([full, partial])
    mat_line = [ln for ln in out.split("<tr") if "full-materialization" in ln][0]
    assert "full / metadata_only / sc on" in mat_line
    assert "partial" not in mat_line


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


def test_summary_overall_returns_html_table():
    recs = load_records([FIX])
    out = summary_overall_table(recs)
    assert out.startswith("<table")
    assert out.rstrip().endswith("</table>")
    # header + exactly one data row (All runs)
    assert out.count("<tr") == 2
    assert "All runs" in out


def test_summary_overall_counts_failed_run_as_zero():
    # one materialized run (recall 1.0) + one failed run (recall None -> 0)
    recs = [
        _mk("a", "metadata_only", True, "m", recall=1.0, materialized=True),
        _mk("a", "metadata_only", True, "m", recall=None, materialized=False),
    ]
    out = summary_overall_table(recs)
    assert ">2<" in out  # n counts both runs
    assert "0.500 / 0.500 ±0.500" in out  # (1.0 + 0)/2 mean, median, sd


def test_summary_by_sc_has_on_and_off_rows():
    recs = [
        _mk("a", "metadata_only", False, "m", recall=0.4, materialized=True),
        _mk("a", "metadata_only", True, "m", recall=1.0, materialized=True),
    ]
    out = summary_by_sc_table(recs)
    assert "<td>off</td>" in out
    assert "<td>on</td>" in out


def test_summary_by_ctx_warns_pooled():
    recs = load_records([FIX])
    out = summary_by_ctx_table(recs)
    assert "pooled" in out
    assert "metadata_only" in out


def _block_recs():
    return [
        _mk("alpha", "metadata_only", True, "m", recall=1.0, fixture="fx1"),
        _mk("alpha", "live_query_tool", True, "m", recall=0.8, fixture="fx2"),
        _mk("beta", "metadata_only", True, "m", recall=0.5, fixture="fx1"),
        _mk("beta", "metadata_only", False, "m", recall=0.2, fixture="fx2"),
    ]


def test_summary_sc_block_on_has_heading_and_both_tables():
    out = summary_sc_block(_block_recs(), sc=True)
    assert "<h3>Self-correction on</h3>" in out
    # strategy table then fixture table, both present
    assert "Per strategy" in out
    assert "Per fixture" in out
    assert out.index("Per strategy") < out.index("Per fixture")


def test_summary_sc_block_filters_to_its_sc_setting():
    out = summary_sc_block(_block_recs(), sc=True)
    # only sc=on runs: alpha (2 ctx runs) + beta (1 run) -> 2 strategies, 2 fixtures
    assert "<td>alpha</td>" in out
    assert "<td>beta</td>" in out
    # alpha mean recall 0.9 sorts above beta 0.5 in the strategy table
    assert out.index("<td>alpha</td>") < out.index("<td>beta</td>")


def test_summary_sc_block_off_excludes_on_runs():
    out = summary_sc_block(_block_recs(), sc=False)
    assert "<h3>Self-correction off</h3>" in out
    # only the single sc=off beta run survives
    assert "<td>beta</td>" in out
    assert "<td>alpha</td>" not in out
