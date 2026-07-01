import asyncio
import pytest
from pydantic_ai.exceptions import UnexpectedModelBehavior
from aidmi_orchestrator.strategy.structured_common import (
    generate_table_mapping_safe, TableMapping,
)
from aidmi_orchestrator.strategy.self_correction import run_dbt_self_correction


class RaisingAgent:
    """An agent whose .run always fails structured output."""
    def __init__(self): self.calls = 0
    async def run(self, prompt, **kw):
        self.calls += 1
        raise UnexpectedModelBehavior("Exceeded maximum output retries (3)")


def test_generate_table_mapping_safe_returns_placeholder_on_output_failure():
    agent = RaisingAgent()
    result = asyncio.run(generate_table_mapping_safe(agent, "Account", "ctx"))
    assert isinstance(result, TableMapping)
    assert result.target_table == "Account"
    assert result.dbt_sql
    assert agent.calls == 1


def test_self_correction_does_not_raise_when_fixer_output_fails(tmp_path):
    """Even via the static gate (unwrapped path), a fixer that can't produce
    structured output must degrade to False, not raise."""
    fixer = RaisingAgent()

    class FakeModel:
        def __init__(self, name, status, msg=""):
            self.model_name = name; self.status = status; self.error_message = msg
    class FakeResult:
        def __init__(self, overall, models): self.overall_status = overall; self.models = models
    class Api:
        async def run_dbt(self):
            return FakeResult("error", [FakeModel("X", "error", "boom")])

    (tmp_path / "models").mkdir()
    mappings = {"X": TableMapping(target_table="X", dbt_sql="SELECT (", column_notes=[])}
    ok = asyncio.run(run_dbt_self_correction(
        Api(), fixer, mappings, "ctx",
        dbt_project_path=tmp_path, source_tables=[("s", "X")], source_schema="s",
        max_passes=2, serial=True, validation_gate="static",
    ))
    assert ok is False
