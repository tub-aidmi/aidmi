"""analysis: aggregate results.jsonl into report tables."""
from __future__ import annotations

import json

from aidmi_orchestrator.analysis import (
    aggregate, load_results, render_markdown, render_matrix, write_csvs,
)


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
    assert "per_table_equality" not in cell.metrics  # nested dicts skipped


def test_aggregate_groups_by_fixture_and_spec() -> None:
    rows = [_row("a", fixture="f1"), _row("a", fixture="f2"), _row("b", fixture="f1")]
    cells = aggregate(rows)
    assert len(cells) == 3


def test_render_markdown_contains_cells_and_metrics() -> None:
    md = render_markdown(aggregate([_row("a"), _row("b", covered=0.25)]))
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


def test_write_csvs(tmp_path) -> None:
    write_csvs(aggregate([_row("a")]), tmp_path)
    cells_csv = (tmp_path / "cells.csv").read_text(encoding="utf-8")
    summary_csv = (tmp_path / "summary.csv").read_text(encoding="utf-8")
    assert "target_columns_covered" in cells_csv
    assert "a" in summary_csv
