import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel

from aidmi_orchestrator.benchmark import Benchmark, expand_grid, parse_strategy_spec
from aidmi_orchestrator.fixtures.base import Fixture


class _NoConfig(BaseModel):
    pass


class _StandinStrategy:
    name = "standin"
    config = _NoConfig()

    async def generate(self, api):
        raise AssertionError("generate must not be reached in these tests")


def _dummy_fixture() -> Fixture:
    return Fixture(
        name="dummy", description="", source_factory=lambda: None,
        target_schema_path=None, reference_dbt_path=None, applicable_evaluators=[],
    )


def test_run_records_harness_errors_instead_of_raising(tmp_path):
    bench = Benchmark(_dummy_fixture(), workspace=tmp_path, staging_db_url="postgresql://nope", evaluators=[])
    with patch("aidmi_orchestrator.benchmark.run_orchestrator", new_callable=AsyncMock, side_effect=OSError("disk full")):
        result = asyncio.run(bench.run(_StandinStrategy(), strategy_spec_name="s", rep_index=2))
    assert result.error is not None
    assert "disk full" in result.error
    assert result.rep_index == 2
    assert (tmp_path / "runs" / result.run_id / "result.json").exists()


def test_evaluator_crash_is_isolated(tmp_path):
    class _BoomEvaluator:
        name = "boom"
        def applies_to(self, artifacts): return True
        def evaluate(self, artifacts): raise ValueError("metric exploded")

    class _OkEvaluator:
        name = "ok"
        def applies_to(self, artifacts): return True
        def evaluate(self, artifacts): return {"fine": 1}

    from aidmi_orchestrator.domain import StrategyResult
    from aidmi_orchestrator.evaluator.base import FixtureMetadata, RunArtifacts
    artifacts = RunArtifacts(
        run_id="r", dbt_project_path=tmp_path, dlt_pipelines_dir=tmp_path,
        staging_db_url="postgresql://", staging_raw_dataset="raw", staging_out_dataset="out",
        trace=[], strategy_result=StrategyResult(target_tables_written=[], self_reported_status="complete"),
        target_schema_input=None,
        fixture=FixtureMetadata(name="f", description="", reference_dbt_path=None, applicable_evaluators=[]),
        wall_clock_seconds=0.0, final_transform_result=None,
    )
    bench = Benchmark(_dummy_fixture(), workspace=tmp_path, staging_db_url="postgresql://",
                      evaluators=[_BoomEvaluator(), _OkEvaluator()])
    with patch("aidmi_orchestrator.benchmark.run_orchestrator", new_callable=AsyncMock, return_value=artifacts):
        result = asyncio.run(bench.run(_StandinStrategy(), strategy_spec_name="s"))
    assert result.metrics["fine"] == 1
    assert "metric exploded" in result.metrics["evaluator_error_boom"]


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
