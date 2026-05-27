import pytest

from aidmi_orchestrator.benchmark import expand_grid, parse_strategy_spec


def test_parse_strategy_spec_round_trip_fields():
    registry, name, cfg = parse_strategy_spec({
        "name": "my_variant",
        "strategy": "structured_per_table",
        "config": {"samples_per_table": 2},
    })
    assert registry == "structured_per_table"
    assert name == "my_variant"
    assert cfg == {"samples_per_table": 2}


def test_parse_strategy_spec_strips_name_whitespace():
    _, name, _ = parse_strategy_spec({"name": "  x  ", "strategy": "mock", "config": {}})
    assert name == "x"


def test_parse_strategy_spec_empty_config():
    _, _, cfg = parse_strategy_spec({"name": "n", "strategy": "mock"})
    assert cfg == {}


def test_parse_strategy_spec_rejects_missing_strategy():
    with pytest.raises(ValueError, match="'strategy'"):
        parse_strategy_spec({"name": "n"})


def test_parse_strategy_spec_rejects_missing_name():
    with pytest.raises(ValueError, match="'name'"):
        parse_strategy_spec({"strategy": "mock"})


def test_parse_strategy_spec_rejects_empty_strings():
    with pytest.raises(ValueError, match="non-empty"):
        parse_strategy_spec({"name": "", "strategy": "mock"})


def test_expand_grid_non_expanding_uses_cell_name():
    spec = {"cells": [{"name": "litellm_try", "strategy": "mock", "config": {"x": 1}}]}
    out = expand_grid(spec)
    assert out == [("mock", {"x": 1}, "litellm_try")]


def test_expand_grid_non_expanding_fallback_to_registry():
    spec = {"cells": [{"strategy": "mock", "config": {"mapping_source": "p.json"}}]}
    out = expand_grid(spec)
    assert out == [("mock", {"mapping_source": "p.json"}, "mock")]


def test_expand_grid_cartesian_suffix():
    spec = {
        "cells": [{
            "name": "spt",
            "strategy": "structured_per_table",
            "config": {
                "writer_model": {},
                "context_mode": ["metadata_only", "metadata_plus_samples"],
            },
        }]
    }
    out = expand_grid(spec)
    labels = {name for _, _, name in out}
    assert labels == {
        "spt_context_mode_metadata_only",
        "spt_context_mode_metadata_plus_samples",
    }


def test_expand_grid_multi_dim_suffix():
    spec = {
        "cells": [{
            "strategy": "mock",
            "config": {"a": [1, 2], "b": [False, True]},
        }]
    }
    out = expand_grid(spec)
    assert len(out) == 4
    labels = {name for _, _, name in out}
    assert "mock_a_1_b_false" in labels
    assert "mock_a_2_b_true" in labels
