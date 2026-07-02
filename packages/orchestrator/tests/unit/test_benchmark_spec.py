import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel

from aidmi_orchestrator.benchmark import Benchmark, expand_grid, parse_strategy_spec, sweep_job_status
from aidmi_orchestrator.domain import BenchmarkResult, StrategyResult
from aidmi_orchestrator.fixtures.base import Fixture


class _NoConfig(BaseModel):
    pass


class _StandinStrategy:
    name = "standin"
    config = _NoConfig()

    async def generate(self, api):
        raise AssertionError("generate must not be reached in these tests")


def _dummy_fixture() -> Fixture:
    from pathlib import Path
    return Fixture(
        name="dummy",
        description="",
        source_schema="fixture_dummy_src",
        source_sql_path=Path("/dev/null"),
        destination_sql_path=Path("/dev/null"),
        target_schema_path=None,
        reference_dbt_path=None,
        applicable_evaluators=[],
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
        staging_db_url="postgresql://", source_schema="raw", out_schema="out",
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
    assert out == [("mock", {"x": 1}, "litellm_try", None)]


def test_expand_grid_non_expanding_fallback_to_registry():
    spec = {"cells": [{"strategy": "mock", "config": {"mapping_source": "p.json"}}]}
    out = expand_grid(spec)
    assert out == [("mock", {"mapping_source": "p.json"}, "mock", None)]


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
    labels = {name for _, _, name, _ in out}
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
    labels = {name for _, _, name, _ in out}
    assert "mock_a_1_b_false" in labels
    assert "mock_a_2_b_true" in labels


def test_expand_grid_resolves_model_refs_cartesian():
    spec = {
        "models": {
            "small": {"provider": "litellm", "model_name": "ise-x/small"},
            "big": {"provider": "litellm", "model_name": "academic/big"},
        },
        "cells": [{
            "name": "spt",
            "strategy": "structured_per_table",
            "config": {"writer_model": ["small", "big"]},
        }],
    }
    out = expand_grid(spec)
    assert len(out) == 2
    by_name = {name: cfg for _, cfg, name, _ in out}
    assert by_name["spt_writer_model_small"]["writer_model"]["model_name"] == "ise-x/small"
    assert by_name["spt_writer_model_big"]["writer_model"]["model_name"] == "academic/big"


def test_expand_grid_resolves_scalar_model_ref():
    spec = {
        "models": {"small": {"provider": "litellm", "model_name": "ise-x/small"}},
        "cells": [{"strategy": "plan_then_execute", "config": {"planner_model": "small"}}],
    }
    (_, cfg, _, _), = expand_grid(spec)
    assert cfg["planner_model"]["model_name"] == "ise-x/small"


def test_expand_grid_inline_model_dict_untouched():
    spec = {"cells": [{"strategy": "structured_per_table",
                       "config": {"writer_model": {"provider": "openai", "model_name": "gpt"}}}]}
    (_, cfg, _, _), = expand_grid(spec)
    assert cfg["writer_model"]["model_name"] == "gpt"


def test_expand_grid_unknown_model_ref_raises():
    spec = {"cells": [{"strategy": "x", "config": {"writer_model": "ghost"}}]}
    with pytest.raises(ValueError, match="ghost"):
        expand_grid(spec)


def test_expand_grid_passes_cell_fixtures_through():
    spec = {"cells": [{"strategy": "mock", "fixtures": ["master"], "config": {}}]}
    (_, _, _, fixtures), = expand_grid(spec)
    assert fixtures == ["master"]


def _bench_result(**kwargs) -> BenchmarkResult:
    from datetime import datetime
    defaults = dict(
        run_id="r1",
        fixture_name="master",
        strategy_name="plan_write_critique",
        strategy_spec_name="spec",
        strategy_config={},
        rep_index=0,
        started_at=datetime.utcnow(),
        completed_at=datetime.utcnow(),
        wall_clock_seconds=1.0,
        strategy_result=StrategyResult(target_tables_written=[], self_reported_status="complete"),
        metrics={},
        error=None,
        source_schema="",
        out_schema="",
    )
    defaults.update(kwargs)
    return BenchmarkResult(**defaults)


def test_sweep_job_status():
    assert sweep_job_status(_bench_result(error="boom")) == "ERROR"
    assert sweep_job_status(_bench_result(
        strategy_result=StrategyResult(target_tables_written=[], self_reported_status="errored"),
    )) == "ERRORED"
    assert sweep_job_status(_bench_result(
        strategy_result=StrategyResult(target_tables_written=[], self_reported_status="gave_up"),
    )) == "GAVE_UP"
    assert sweep_job_status(_bench_result(
        strategy_result=StrategyResult(target_tables_written=["a"], self_reported_status="partial"),
    )) == "PARTIAL"
    assert sweep_job_status(_bench_result(metrics={"dbt_success": False})) == "FAIL"
    assert sweep_job_status(_bench_result(metrics={"dbt_success": True})) == "ok"
