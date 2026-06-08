"""Real-LLM smoke: one run per new strategy against sp1_users via the ISE proxy.

Requires LITELLM_API_KEY and the SSH tunnel (localhost:4000). Skipped otherwise.
"""
from __future__ import annotations

import asyncio
import os

import pytest

import aidmi_orchestrator.strategy  # noqa: F401
import aidmi_orchestrator.evaluator  # noqa: F401
import aidmi_orchestrator.fixtures  # noqa: F401

from aidmi_orchestrator.benchmark import Benchmark
from aidmi_orchestrator.fixtures.base import get_fixture
from aidmi_orchestrator.strategy.base import make_strategy

pytestmark = pytest.mark.requires_llm

BASE_URL = os.environ.get("AIDMI_SMOKE_BASE_URL", "http://localhost:4000/v1")
MODEL = os.environ.get("AIDMI_SMOKE_MODEL", "ise-ollama/qwen3.6:35b-a3b")


def _spec() -> dict:
    return {
        "provider": "litellm", "model_name": MODEL,
        "base_url": BASE_URL, "api_key_env": "LITELLM_API_KEY",
    }


CASES = [
    ("structured_per_table", {"writer_model": _spec(), "enable_self_correction": True}),
    ("write_then_critique", {"writer_model": _spec(), "max_critique_rounds": 1}),
    ("plan_then_execute", {"planner_model": _spec()}),
    ("ensemble_vote", {"writer_model": _spec(), "n_candidates": 2}),
]


@pytest.mark.skipif(not os.environ.get("LITELLM_API_KEY"), reason="LITELLM_API_KEY not set")
@pytest.mark.parametrize("registry,config", CASES, ids=[c[0] for c in CASES])
def test_strategy_smoke(registry, config, staging_db_url, tmp_path):
    strategy = make_strategy(registry, config)
    bench = Benchmark(get_fixture("sp1_users"), workspace=tmp_path, staging_db_url=staging_db_url)
    result = asyncio.run(bench.run(strategy, strategy_spec_name=f"smoke_{registry}"))
    assert result.error is None
    assert result.strategy_result.target_tables_written
    assert result.metrics.get("manifest_present") is True
