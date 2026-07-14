from aidmi_orchestrator.report.aggregate import (
    group_mean,
    pass_rate,
    rep_values,
    summary_stats,
)
from aidmi_orchestrator.report.data import RunRecord


def _r(cell, recall, mat):
    return RunRecord(campaign="c", model="m", fixture="f", cell=cell, ctx="metadata_only",
        sc=True, rep=0, dbt_success=mat, materialized=mat,
        tables_materialized=1.0 if mat else 0.0,
        recall=recall, precision=None, field_acc=None,
        f1=None, cost=None, secs=None, tokens_in=None, tokens_out=None,
        status="complete", silent_fail=False, tables_declared=5, cols_covered=None)


def test_group_mean_skips_none():
    recs = [_r("a", 1.0, True), _r("a", None, True), _r("a", 0.0, False)]
    assert group_mean(recs, lambda r: r.cell, lambda r: r.recall)["a"] == 0.5


def test_pass_rate():
    recs = [_r("a", 1.0, True), _r("a", 1.0, False)]
    assert pass_rate(recs, lambda r: r.cell, lambda r: r.materialized)["a"] == 0.5


def test_rep_values_collects_non_none():
    recs = [_r("a", 1.0, True), _r("a", None, True)]
    assert rep_values(recs, lambda r: r.cell, lambda r: r.recall)["a"] == [1.0]


def test_summary_stats_empty_is_none():
    assert summary_stats([]) is None


def test_summary_stats_single_value_zero_sd():
    assert summary_stats([0.5]) == (0.5, 0.5, 0.0)


def test_summary_stats_mean_median_sd():
    mean, median, sd = summary_stats([0.0, 0.0, 1.0, 1.0])
    assert mean == 0.5
    assert median == 0.5
    assert sd == 0.5
