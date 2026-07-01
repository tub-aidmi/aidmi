import asyncio

import pytest
from aidmi_orchestrator.strategy.write_tools_freeform.self_correction import (
    run_post_agent_dbt_loop,
)

class FakeAgent:
    def __init__(self): self.calls = 0
    async def run(self, prompt, **kw): self.calls += 1

class FakeResult:
    def __init__(self, overall): self.overall_status = overall; self.models = []

class Api:
    def __init__(self, models_dir):
        self.source_schema = "s"
        self.dbt_project_path = models_dir.parent
        self._n = 0
    async def run_dbt(self):
        self._n += 1
        return FakeResult("success")

def test_static_gate_prompts_fixer_before_success(tmp_path):
    models = tmp_path / "models"; models.mkdir()
    (models / "Account.sql").write_text("SELECT (", encoding="utf-8")
    from pydantic_ai import UsageLimits
    fixer = FakeAgent()
    ok = asyncio.run(run_post_agent_dbt_loop(
        Api(models), FakeAgent(), UsageLimits(request_limit=5),
        max_passes=3, fixer_agent=fixer, validation_gate="static",
    ))
    assert ok is True
    assert fixer.calls >= 1
