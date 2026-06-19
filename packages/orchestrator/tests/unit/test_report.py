"""report: aggregate results.jsonl into report tables."""
from __future__ import annotations

import json

import aidmi_orchestrator.report  # noqa: F401
from aidmi_orchestrator.report.aggregate import CellAggregate, aggregate, load_results
from aidmi_orchestrator.report.layout.benchmark_grid import (
    build_strategy_model_matrix,
    heatmap_cell_base,
    heatmap_row_key,
    heatmap_row_label,
)
from aidmi_orchestrator.report.render.markdown import render_markdown, render_matrix
from aidmi_orchestrator.report.render.tables import write_tables


def _row(spec: str, fixture: str = "fx", model: str = "ise-x/q", *,
         covered: float = 1.0, error: str | None = None, rep: int = 0) -> dict:
    return {
        "run_id": "r", "fixture_name": fixture, "strategy_name": spec.split("_rep")[0],
        "strategy_spec_name": spec, "rep_index": rep,
        "strategy_config": {"writer_model": {"provider": "litellm", "model_name": model}},
        "wall_clock_seconds": 10.0,
        "strategy_result": {"target_tables_written": [], "self_reported_status": "complete"},
        "metrics": {"dbt_success": error is None, "target_columns_covered": covered,
                    "per_table_equality": {"users": {}}},
        "error": error,
    }


def test_load_results_accepts_files_and_dirs(tmp_path) -> None:
    f = tmp_path / "results.jsonl"
    f.write_text(json.dumps(_row("a")) + "\n", encoding="utf-8")
    assert len(load_results([f])) == 1
    assert len(load_results([tmp_path])) == 1


def test_aggregate_means_stds_and_rates() -> None:
    rows = [
        _row("a", covered=1.0, rep=0),
        _row("a", covered=0.5, rep=1),
        _row("a", covered=0.0, error="harness: boom", rep=2),
    ]
    (cell,) = aggregate(rows)
    assert cell.spec_name == "a"
    assert cell.model_name == "ise-x/q"
    assert cell.n_runs == 3
    assert cell.metrics["target_columns_covered"]["mean"] == 0.5
    assert cell.metrics["dbt_success"]["mean"] == 2 / 3
    assert cell.metrics["ran_ok"]["mean"] == 2 / 3
    assert cell.metrics["wall_clock_seconds"]["mean"] == 10.0
    assert "per_table_equality" not in cell.metrics


def test_aggregate_groups_by_fixture_and_spec() -> None:
    rows = [_row("a", fixture="f1"), _row("a", fixture="f2"), _row("b", fixture="f1")]
    cells = aggregate(rows)
    assert len(cells) == 3


def test_render_markdown_contains_cells_and_metrics() -> None:
    md = render_markdown(aggregate([_row("a"), _row("b", covered=0.25)]), ["target_columns_covered"])
    assert "## fx" in md
    assert "| a " in md and "| b " in md
    assert "target_columns_covered" in md


def test_render_matrix_strategy_by_model() -> None:
    rows = [
        _row("s1_qwen", model="ise-x/q"),
        _row("s1_big", model="academic/big", covered=0.5),
    ]
    for r in rows:
        r["strategy_name"] = "s1"
    matrix = render_matrix(aggregate(rows), "target_columns_covered")
    assert "ise-x/q" in matrix and "academic/big" in matrix
    assert "s1" in matrix


def test_write_tables(tmp_path) -> None:
    write_tables(aggregate([_row("a")]), tmp_path)
    cells_csv = (tmp_path / "cells.csv").read_text(encoding="utf-8")
    summary_csv = (tmp_path / "summary.csv").read_text(encoding="utf-8")
    assert "target_columns_covered" in cells_csv
    assert "a" in summary_csv


def test_heatmap_cell_base_strips_model_suffix() -> None:
    assert heatmap_cell_base("structured_sc_writer_model_qwen36") == "structured_sc"
    assert heatmap_cell_base("plan_planner_model_qwen397") == "plan"
    assert heatmap_cell_base("mock_control") is None


def test_heatmap_row_key_and_label() -> None:
    assert heatmap_row_key("structured_sc_writer_model_qwen36", "structured_per_table") == "structured_sc"
    assert heatmap_row_label("structured_sc", "structured_per_table") == "structured_per_table_sc"
    assert heatmap_row_key("mock_control", "mock") == "mock"
    assert heatmap_row_label("mock", "mock") == "mock"


def test_build_strategy_model_matrix() -> None:
    cells = [
        CellAggregate(
            fixture_name="fx",
            spec_name="critique_writer_model_qwen36",
            strategy_name="write_then_critique",
            model_name="ise-ollama/qwen3.6:35b-a3b",
            n_runs=3,
            metrics={"target_columns_covered": {"mean": 1.0, "std": 0.0, "n": 3.0}},
        ),
        CellAggregate(
            fixture_name="fx",
            spec_name="critique_writer_model_qwen9b",
            strategy_name="write_then_critique",
            model_name="ise-openai-nvidia/qwen35-9b",
            n_runs=3,
            metrics={"target_columns_covered": {"mean": 0.0, "std": 0.0, "n": 3.0}},
        ),
    ]
    built = build_strategy_model_matrix(cells, "fx", "target_columns_covered")
    assert built is not None
    matrix, std_matrix, n_matrix, row_labels, col_labels = built
    assert row_labels == ["write_then_critique"]
    assert "qwen3.6-35b" in col_labels
    assert matrix[0, col_labels.index("qwen3.6-35b")] == 1.0
    assert matrix[0, col_labels.index("qwen3.5-9b")] == 0.0
    assert n_matrix[0, col_labels.index("qwen3.6-35b")] == 3.0


def test_build_strategy_model_matrix_missing_cell_is_nan() -> None:
    cells = [
        CellAggregate(
            fixture_name="fx",
            spec_name="critique_writer_model_qwen36",
            strategy_name="write_then_critique",
            model_name="ise-ollama/qwen3.6:35b-a3b",
            n_runs=1,
            metrics={"dbt_success": {"mean": 1.0, "std": 0.0, "n": 1.0}},
        ),
        CellAggregate(
            fixture_name="fx",
            spec_name="plan_planner_model_qwen9b",
            strategy_name="plan_then_execute",
            model_name="ise-openai-nvidia/qwen35-9b",
            n_runs=1,
            metrics={"dbt_success": {"mean": 0.0, "std": 0.0, "n": 1.0}},
        ),
    ]
    built = build_strategy_model_matrix(cells, "fx", "dbt_success")
    assert built is not None
    matrix, _std_matrix, _n_matrix, row_labels, col_labels = built
    assert "write_then_critique" in row_labels
    assert "qwen3.5-9b" in col_labels
    import numpy as np
    critique_i = row_labels.index("write_then_critique")
    qwen9b_j = col_labels.index("qwen3.5-9b")
    assert np.isnan(matrix[critique_i, qwen9b_j])
