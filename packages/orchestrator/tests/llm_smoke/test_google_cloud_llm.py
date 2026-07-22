"""Smoke test against Google Cloud (Agent Platform). Skipped unless GOOGLE_API_KEY is set."""

import asyncio
import os

import pytest

import aidmi_orchestrator.evaluator  # noqa: F401
import aidmi_orchestrator.fixtures  # noqa: F401
import aidmi_orchestrator.strategy  # noqa: F401
from aidmi_orchestrator.benchmark import Benchmark
from aidmi_orchestrator.fixtures.base import get_fixture
from aidmi_orchestrator.scripts.init_fixtures import init_fixture
from aidmi_orchestrator.strategy.base import make_strategy

pytestmark = pytest.mark.requires_llm


@pytest.mark.skipif(
    "GOOGLE_API_KEY" not in os.environ,
    reason="GOOGLE_API_KEY not set",
)
def test_write_tools_freeform_google_cloud_smoke(staging_db_url, tmp_path):
    init_fixture("mock", staging_db_url)
    strategy = make_strategy(
        "write_tools_freeform",
        {
            "writer_model": {
                "provider": "google_cloud",
                "model_name": "gemini-2.5-flash",
                "api_key_env": "GOOGLE_API_KEY",
                "extra": {
                    "google_thinking_config": {"thinking_budget": 2048},
                },
            },
            "context_mode": "metadata_plus_samples",
            "samples_per_table": 3,
            "max_tool_turns": 20,
            "enable_self_correction": False,
        },
    )
    bench = Benchmark(
        get_fixture("mock"), workspace=tmp_path, staging_db_url=staging_db_url
    )
    result = asyncio.run(
        bench.run(
            strategy, strategy_spec_name="write_tools_freeform_google_cloud_smoke"
        )
    )

    assert result.error is None, f"orchestrator errored: {result.error}"
    assert result.strategy_result.target_tables_written
    assert result.metrics["llm_calls_total"] >= 1
    assert result.metrics["dollar_cost_total"] >= 0
    assert "tokens_thoughts_total" in result.metrics
    assert "context_utilization_peak" in result.metrics
    assert "usage_details_total" in result.metrics
    assert result.metrics["tokens_thoughts_total"] >= 0
    assert result.metrics["context_utilization_peak"] >= 0
