from __future__ import annotations

from aidmi_orchestrator.report.aggregate import build_rep_series


def _row(spec: str, fixture: str = "fx", model: str = "m1", covered: float = 1.0, rep: int = 0) -> dict:
    return {
        "fixture_name": fixture,
        "strategy_name": "write_then_critique",
        "strategy_spec_name": spec,
        "rep_index": rep,
        "strategy_config": {"writer_model": {"model_name": model}},
        "wall_clock_seconds": 10.0,
        "metrics": {"target_columns_covered": covered},
        "error": None,
    }


def test_build_rep_series_groups_reps() -> None:
    rows = [
        _row("critique_writer_model_qwen36", covered=1.0, rep=0),
        _row("critique_writer_model_qwen36", covered=0.5, rep=1),
    ]
    series = build_rep_series(rows)
    matching = [
        s for s in series
        if s.fixture_name == "fx" and s.row_key == "critique" and s.metric == "target_columns_covered"
    ]
    assert len(matching) == 1
    assert matching[0].values == [1.0, 0.5]
