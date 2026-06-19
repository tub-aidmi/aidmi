from __future__ import annotations

import csv

import pytest

import aidmi_orchestrator.report  # noqa: F401
from aidmi_orchestrator.report.aggregate import CellAggregate, aggregate, build_rep_series
from aidmi_orchestrator.report.catalog import build_report_plan
from aidmi_orchestrator.report.format import fmt_mean_std, mean_plot_title
from aidmi_orchestrator.report.render.heatmap import HeatmapPlotSpec, build_global_heatmap_spec, render_heatmap_svg
from aidmi_orchestrator.report.render.plot_csv import write_plot_csv
from aidmi_orchestrator.report.render.plots import render_plot_artifact, write_plots


def _cells() -> list[CellAggregate]:
    return [
        CellAggregate(
            fixture_name="fx",
            spec_name="critique_writer_model_qwen36",
            strategy_name="write_then_critique",
            model_name="ise-ollama/qwen3.6:35b-a3b",
            n_runs=2,
            metrics={"target_columns_covered": {"mean": 0.75, "std": 0.25, "n": 2.0}},
        ),
    ]


def test_heatmap_plot_csv_matches_matrix(tmp_path) -> None:
    plan = build_report_plan()
    descriptor = plan.descriptors["target_columns_covered"]
    spec = build_global_heatmap_spec(_cells(), "fx", "target_columns_covered", descriptor)
    assert spec is not None
    csv_path = tmp_path / "plot.csv"
    write_plot_csv(spec, csv_path)
    rows = list(csv.DictReader(csv_path.read_text(encoding="utf-8").splitlines()))
    assert rows
    assert {"strategy", "model", "value", "std", "n"} <= set(rows[0].keys())
    row = next(r for r in rows if r["value"] == "0.75")
    assert row["std"] == "0.25"
    assert row["n"] == "2"


def test_heatmap_spec_populates_std_and_n() -> None:
    plan = build_report_plan()
    descriptor = plan.descriptors["target_columns_covered"]
    spec = build_global_heatmap_spec(_cells(), "fx", "target_columns_covered", descriptor)
    assert spec is not None
    assert spec.std[0, 0] == 0.25
    assert spec.n[0, 0] == 2.0


def test_heatmap_title_n1_omits_mean(tmp_path) -> None:
    pytest.importorskip("matplotlib")
    plan = build_report_plan()
    descriptor = plan.descriptors["target_columns_covered"]
    cells = [
        CellAggregate(
            fixture_name="fx",
            spec_name="critique_writer_model_qwen36",
            strategy_name="write_then_critique",
            model_name="ise-ollama/qwen3.6:35b-a3b",
            n_runs=1,
            metrics={"target_columns_covered": {"mean": 1.0, "std": 0.0, "n": 1.0}},
        ),
    ]
    spec = build_global_heatmap_spec(cells, "fx", "target_columns_covered", descriptor)
    assert spec is not None
    svg_path = tmp_path / "n1.svg"
    render_heatmap_svg(spec, svg_path)
    svg = svg_path.read_text(encoding="utf-8")
    assert mean_plot_title("target_columns_covered", "fx", 1) in svg
    assert "Mean target_columns_covered" not in svg


def test_heatmap_two_line_cell_when_n_gt_1(tmp_path) -> None:
    pytest.importorskip("matplotlib")
    plan = build_report_plan()
    descriptor = plan.descriptors["target_columns_covered"]
    spec = build_global_heatmap_spec(_cells(), "fx", "target_columns_covered", descriptor)
    assert spec is not None
    mean_line, std_line = fmt_mean_std(0.75, 0.25, n=2, rate=True)
    assert std_line is not None
    svg_path = tmp_path / "n2.svg"
    render_heatmap_svg(spec, svg_path)
    svg = svg_path.read_text(encoding="utf-8")
    assert mean_line in svg
    assert std_line in svg


def test_render_plot_artifact_writes_svg_and_csv(tmp_path) -> None:
    pytest.importorskip("matplotlib")
    plan = build_report_plan()
    descriptor = plan.descriptors["target_columns_covered"]
    spec = build_global_heatmap_spec(_cells(), "fx", "target_columns_covered", descriptor)
    assert spec is not None
    paths = render_plot_artifact(spec, tmp_path / "target_columns_covered")
    suffixes = {p.suffix for p in paths}
    assert suffixes == {".svg", ".csv"}
    assert all(p.exists() for p in paths)


def test_write_plots_emits_pairs(tmp_path) -> None:
    pytest.importorskip("matplotlib")
    rows = [{
        "fixture_name": "fx",
        "strategy_name": "write_then_critique",
        "strategy_spec_name": "critique_writer_model_qwen36",
        "strategy_config": {"writer_model": {"model_name": "ise-ollama/qwen3.6:35b-a3b"}},
        "wall_clock_seconds": 1.0,
        "metrics": {"target_columns_covered": 1.0, "dbt_success": True},
        "error": None,
    }]
    cells = aggregate(rows)
    series = build_rep_series(rows)
    plan = build_report_plan()
    artifacts = write_plots(cells, series, rows, tmp_path, plan)
    svgs = [p for p in artifacts if p.suffix == ".svg"]
    csvs = [p for p in artifacts if p.suffix == ".csv"]
    assert len(svgs) == len(csvs)
    assert (tmp_path / "fx" / "global" / "target_columns_covered.svg").exists()
    assert (tmp_path / "fx" / "global" / "target_columns_covered.csv").exists()
