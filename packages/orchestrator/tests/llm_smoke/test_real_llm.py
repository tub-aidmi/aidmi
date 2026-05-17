"""Smoke test against a real LLM. Skipped unless OPENAI_API_KEY is set."""
import os
import asyncio
import pytest

import aidmi_orchestrator.strategy  # noqa: F401
import aidmi_orchestrator.evaluator  # noqa: F401
import aidmi_orchestrator.fixtures  # noqa: F401

from aidmi_orchestrator.fixtures.base import get_fixture
from aidmi_orchestrator.strategy.base import make_strategy
from aidmi_orchestrator.benchmark import Benchmark


pytestmark = pytest.mark.requires_llm


@pytest.mark.skipif(
    "OPENAI_API_KEY" not in os.environ,
    reason="OPENAI_API_KEY not set",
)
def test_structured_per_table_openai_smoke(staging_db_url, tmp_path):
    fixture = get_fixture("sp1_users")
    strategy = make_strategy("structured_per_table", {
        "writer_model": {
            "provider": "openai",
            "model_name": "gpt-4o-mini",
            "api_key_env": "OPENAI_API_KEY",
        },
        "context_mode": "metadata_plus_samples",
        "samples_per_table": 3,
    })
    bench = Benchmark(fixture, workspace=tmp_path, staging_db_url=staging_db_url)
    result = asyncio.run(bench.run(strategy))

    assert result.error is None, f"orchestrator errored: {result.error}"
    assert result.metrics["dbt_success"] is True, (
        f"dbt failed; errors: {result.metrics.get('dbt_error_messages')}"
    )
    # Semantic equivalence allowed via fuzzy comparator defaults at sweep level;
    # the integration-test ExactComparator is too strict for stochastic LLM output.
    # For the smoke test we only require row_count_match.
    assert result.metrics["row_count_match"] is True
    assert result.metrics["llm_calls_total"] >= 1
    assert result.metrics["dollar_cost_total"] >= 0
