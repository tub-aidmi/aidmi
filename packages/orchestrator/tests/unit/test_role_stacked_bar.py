from __future__ import annotations

import csv

import pytest

from aidmi_orchestrator.report.render.plot_csv import role_stacked_bar_csv_rows, write_plot_csv
from aidmi_orchestrator.report.render.stacked_bar import render_stacked_bar_svg
from aidmi_orchestrator.report.role_aggregate import (
    ROLE_STACKED_METRICS,
    build_all_role_stacked_bar_specs,
    build_role_stacked_bar_spec,
    multi_role_strategies,
)


def _row(
    *,
    fixture: str = "fx",
    spec: str = "critique_writer_model_qwen36",
    strategy: str = "write_then_critique",
    model: str = "ise-ollama/qwen3.6:35b-a3b",
    metrics: dict | None = None,
    rep: int = 0,
) -> dict:
    return {
        "fixture_name": fixture,
        "strategy_name": strategy,
        "strategy_spec_name": spec,
        "rep_index": rep,
        "strategy_config": {"writer_model": {"model_name": model}},
        "wall_clock_seconds": 1.0,
        "metrics": metrics or {},
        "error": None,
    }


def _multi_role_metrics(
    writer_in: int,
    writer_out: int,
    critic_in: int,
    critic_out: int,
    writer_calls: int = 1,
    critic_calls: int = 1,
) -> dict:
    return {
        "tokens_input_by_role": {"writer": writer_in, "critic": critic_in},
        "tokens_output_by_role": {"writer": writer_out, "critic": critic_out},
        "llm_calls_by_role": {"writer": writer_calls, "critic": critic_calls},
    }


def test_multi_role_strategies_detects_critique() -> None:
    rows = [
        _row(metrics=_multi_role_metrics(100, 50, 80, 40)),
        _row(spec="structured_writer_model_qwen36", strategy="structured_per_table",
             metrics={"tokens_input_by_role": {"writer": 100}}),
    ]
    assert multi_role_strategies(rows) == ["write_then_critique"]


def test_build_role_stacked_bar_spec_means_across_reps() -> None:
    rows = [
        _row(metrics=_multi_role_metrics(100, 50, 80, 40)),
        _row(rep=1, metrics=_multi_role_metrics(200, 100, 120, 60)),
        _row(
            spec="critique_writer_model_qwen9b",
            model="ise-openai-nvidia/qwen35-9b",
            metrics=_multi_role_metrics(300, 150, 90, 45, writer_calls=2, critic_calls=1),
        ),
    ]
    spec = build_role_stacked_bar_spec(rows, "fx", "write_then_critique", "tokens_input_by_role")
    assert spec is not None
    assert spec.role_labels == ["writer", "critic"]
    assert "qwen3.6-35b" in spec.col_labels
    assert "qwen3.5-9b" in spec.col_labels

    qwen36_i = spec.col_labels.index("qwen3.6-35b")
    qwen9b_i = spec.col_labels.index("qwen3.5-9b")
    assert spec.segments[qwen36_i]["writer"] == 150.0
    assert spec.segments[qwen36_i]["critic"] == 100.0
    assert spec.totals[qwen36_i] == 250.0
    assert spec.segments[qwen9b_i]["writer"] == 300.0
    assert spec.totals[qwen9b_i] == 390.0


def test_build_role_stacked_bar_spec_skips_missing_metric() -> None:
    rows = [_row(metrics={"llm_calls_by_role": {"writer": 1, "critic": 1}})]
    assert build_role_stacked_bar_spec(rows, "fx", "write_then_critique", "tokens_input_by_role") is None


def test_build_all_role_stacked_bar_specs_emits_three_metrics() -> None:
    rows = [
        _row(metrics=_multi_role_metrics(100, 50, 80, 40)),
        _row(
            spec="plan_planner_model_qwen36",
            strategy="plan_then_execute",
            metrics={
                "tokens_input_by_role": {"planner": 50, "writer": 100},
                "tokens_output_by_role": {"planner": 20, "writer": 40},
                "llm_calls_by_role": {"planner": 1, "writer": 2},
            },
        ),
    ]
    specs = build_all_role_stacked_bar_specs(rows)
    metrics = {(s.strategy, s.metric) for s in specs}
    for strategy in ("write_then_critique", "plan_then_execute"):
        for metric in ("tokens_input_by_role", "tokens_output_by_role", "llm_calls_by_role"):
            assert (strategy, metric) in metrics


def test_role_stacked_bar_csv_rows() -> None:
    rows_data = [
        _row(metrics=_multi_role_metrics(100, 50, 80, 40)),
    ]
    spec = build_role_stacked_bar_spec(rows_data, "fx", "write_then_critique", "llm_calls_by_role")
    assert spec is not None
    rows = role_stacked_bar_csv_rows(spec)
    assert rows
    assert {"strategy", "model", "role", "value", "total"} <= set(rows[0].keys())
    assert any(r["role"] == "writer" and float(r["value"]) == 1.0 for r in rows)


def test_role_dict_metrics_latency_sum() -> None:
    from aidmi_orchestrator.report.role_aggregate import role_dict_metrics

    row = _row(metrics={"latency_ms_sum_by_role": {"writer": 100.0, "critic": 50.0}})
    assert role_dict_metrics(row, "latency_ms_sum_by_role") == {"writer": 100.0, "critic": 50.0}


def test_render_stacked_bar_svg(tmp_path) -> None:
    pytest.importorskip("matplotlib")
    rows = [_row(metrics=_multi_role_metrics(1000, 500, 800, 400))]
    spec = build_role_stacked_bar_spec(rows, "fx", "write_then_critique", "tokens_input_by_role")
    assert spec is not None
    svg_path = tmp_path / "tokens_input_by_role.svg"
    render_stacked_bar_svg(spec, svg_path)
    assert svg_path.exists()
    write_plot_csv(spec, tmp_path / "tokens_input_by_role.csv")
    csv_rows = list(csv.DictReader((tmp_path / "tokens_input_by_role.csv").read_text(encoding="utf-8").splitlines()))
    assert len(csv_rows) == 2
