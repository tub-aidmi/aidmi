"""Failure-accounting: quality means reported both with and without failed runs."""

from __future__ import annotations

from types import SimpleNamespace

from aidmi_orchestrator.report.aggregate import group_mean, group_mean_zero
from aidmi_orchestrator.report.tables import failure_accounting_table


def _r(cell, recall, f1=None, materialized=True):
    return SimpleNamespace(cell=cell, recall=recall, f1=f1, materialized=materialized)


def test_group_mean_drops_none_but_zero_variant_counts_it():
    recs = [_r("a", 1.0), _r("a", None), _r("a", 0.5)]
    key = lambda r: r.cell  # noqa: E731
    metric = lambda r: r.recall  # noqa: E731
    # Evaluated-only mean ignores the failed (None) run.
    assert group_mean(recs, key, metric)["a"] == 0.75
    # Including-failed mean counts the failed run as 0.
    assert group_mean_zero(recs, key, metric)["a"] == 0.5


def test_group_mean_zero_all_none_is_zero_not_dropped():
    recs = [_r("a", None), _r("a", None)]
    key = lambda r: r.cell  # noqa: E731
    metric = lambda r: r.recall  # noqa: E731
    assert "a" not in group_mean(recs, key, metric)  # nothing to average
    assert group_mean_zero(recs, key, metric)["a"] == 0.0


def test_failure_accounting_table_reports_both_and_failed_count():
    recs = [
        _r("alpha", 1.0, 1.0),
        _r("alpha", 0.0, None, materialized=False),  # materialized nothing
        _r("alpha", 0.5, 0.6),
        _r("beta", 0.8, 0.9),
    ]
    html = failure_accounting_table(recs)
    # Both strategies present.
    assert "alpha" in html and "beta" in html
    # alpha recall: eval-only (1.0+0.0+0.5)/3=0.500 (0.0 already counted),
    # incl-failed same 0.500.
    # alpha f1: evaluated-only drops the null -> (1.0+0.6)/2=0.800;
    # incl-failed counts it as 0 -> (1.0+0+0.6)/3=0.533.
    assert "0.800" in html  # evaluated-only f1
    assert "0.533" in html  # including-failed f1
    # The failed-run count is surfaced.
    assert ">1<" in html  # one failed run for alpha
