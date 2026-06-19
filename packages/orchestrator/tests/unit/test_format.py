from __future__ import annotations

from aidmi_orchestrator.report.format import fmt_agg, fmt_mean_std, mean_plot_title, uniform_plot_n


def test_fmt_agg_with_std() -> None:
    assert fmt_agg({"mean": 0.75, "std": 0.25, "n": 2.0}) == "0.75±0.25"


def test_fmt_agg_without_std() -> None:
    assert fmt_agg({"mean": 1.0, "std": 0.0, "n": 1.0}) == "1"


def test_fmt_mean_std_two_lines() -> None:
    mean_line, std_line = fmt_mean_std(0.75, 0.25, n=2, rate=True)
    assert mean_line == "0.75"
    assert std_line == "±0.25"


def test_fmt_mean_std_single_line_when_n1() -> None:
    mean_line, std_line = fmt_mean_std(0.75, 0.0, n=1, rate=True)
    assert mean_line == "0.75"
    assert std_line is None


def test_mean_plot_title_n1() -> None:
    assert mean_plot_title("target_columns_covered", "fx", 1) == "target_columns_covered — fx"


def test_mean_plot_title_n_gt_1() -> None:
    assert mean_plot_title("target_columns_covered", "fx", 3) == (
        "Mean target_columns_covered (n=3) — fx"
    )


def test_uniform_plot_n() -> None:
    assert uniform_plot_n([3, 3, 3]) == 3
    assert uniform_plot_n([1, 3]) is None
