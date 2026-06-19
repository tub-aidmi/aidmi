from __future__ import annotations

import pytest

from aidmi_orchestrator.report.strategy_plots import (
    FUNNEL_PRESERVATION_ROW_RATIO_MIN,
    FUNNEL_TARGET_COLUMNS_MIN,
    build_funnel_specs,
    build_grouped_bar_specs,
    build_rep_stability_specs,
    build_row_equality_heatmap_specs,
    build_self_correction_specs,
    strategy_label,
)


def _row(
    *,
    fixture: str = "fx",
    spec: str = "structured_writer_model_qwen36",
    strategy: str = "structured_per_table",
    model: str = "ise-ollama/qwen3.6:35b-a3b",
    metrics: dict | None = None,
    error: str | None = None,
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
        "error": error,
    }


def test_strategy_label_maps_sc_suffix() -> None:
    row = _row(spec="structured_sc_writer_model_qwen36", strategy="structured_per_table")
    assert strategy_label(row) == "structured_per_table_sc"


def test_build_funnel_specs_stage_rates() -> None:
    rows = [
        _row(metrics={"dbt_success": True, "target_columns_covered": 1.0}, rep=0),
        _row(metrics={"dbt_success": True, "target_columns_covered": 0.5}, rep=1),
        _row(
            spec="structured_writer_model_qwen9b",
            model="ise-openai-nvidia/qwen35-9b",
            metrics={"dbt_success": False},
            rep=0,
        ),
    ]
    specs = build_funnel_specs(rows)
    assert len(specs) == 1
    spec = specs[0]
    assert "ran_ok" in spec.stage_labels
    assert "schema_ok" in spec.stage_labels
    qwen36_i = spec.col_labels.index("qwen3.6-35b")
    schema_i = spec.stage_labels.index("schema_ok")
    assert spec.pass_rates[schema_i][qwen36_i] == 0.5


def test_build_grouped_bar_preservation_profile() -> None:
    rows = [
        _row(metrics={
            "preservation_row_ratio_mean": 1.0,
            "preservation_distinct_ratio_mean": 0.9,
            "preservation_null_inflation_mean": 0.01,
        }),
    ]
    specs = [s for s in build_grouped_bar_specs(rows) if s.plot_id == "preservation_profile"]
    assert len(specs) == 1
    assert specs[0].series_labels == [
        "preservation_row_ratio_mean",
        "preservation_distinct_ratio_mean",
        "preservation_null_inflation_mean",
    ]
    assert specs[0].n_by_model == [1]


def test_build_rep_stability_requires_two_reps() -> None:
    rows = [_row(metrics={"dbt_success": 1.0})]
    assert build_rep_stability_specs(rows) == []
    rows.append(_row(metrics={"dbt_success": 0.0}, rep=1))
    specs = build_rep_stability_specs(rows)
    assert any(s.metric == "dbt_success" for s in specs)


def test_build_preservation_per_table() -> None:
    rows = [_row(metrics={"preservation_per_table": {"users": {"row_ratio": 0.9}}})]
    specs = [s for s in build_grouped_bar_specs(rows) if s.plot_id == "preservation_per_table"]
    assert len(specs) == 1
    assert specs[0].series_labels == ["users"]


def test_build_row_equality_heatmap() -> None:
    rows = [
        _row(metrics={"per_table_equality": {"users": {"row_count_match": True}}}, rep=0),
        _row(metrics={"per_table_equality": {"users": {"row_count_match": False}}}, rep=1),
    ]
    specs = build_row_equality_heatmap_specs(rows)
    assert len(specs) == 1
    assert specs[0].row_labels == ["users"]
    assert specs[0].n[0, 0] == 2
    assert specs[0].std[0, 0] == pytest.approx(0.707, rel=1e-2)


def test_build_self_correction_dumbbell() -> None:
    rows = [
        _row(
            spec="structured_writer_model_qwen36",
            strategy="structured_per_table",
            metrics={"dbt_success": True, "target_columns_covered": 0.8},
        ),
        _row(
            spec="structured_sc_writer_model_qwen36",
            strategy="structured_per_table",
            metrics={"dbt_success": True, "target_columns_covered": 1.0},
        ),
    ]
    specs = build_self_correction_specs(rows)
    assert any(s.metric == "target_columns_covered" for s in specs)
    spec = next(s for s in specs if s.metric == "target_columns_covered")
    assert spec.base_values[0] == 0.8
    assert spec.variant_values[0] == 1.0


def test_funnel_threshold_constants() -> None:
    assert 0 < FUNNEL_TARGET_COLUMNS_MIN < 1
    assert 0 < FUNNEL_PRESERVATION_ROW_RATIO_MIN <= 1
