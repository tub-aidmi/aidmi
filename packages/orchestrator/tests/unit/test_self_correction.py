import asyncio

from aidmi_orchestrator.strategy.self_correction import run_dbt_self_correction
from aidmi_orchestrator.strategy.structured_common import TableMapping


class FakeRun:
    def __init__(self, output):
        self.output = output


class FakeAgent:
    """Records prompts; returns a valid TableMapping each time."""

    def __init__(self, tag):
        self.tag = tag
        self.calls = 0

    async def run(self, prompt, **kw):
        self.calls += 1
        return FakeRun(
            TableMapping(
                target_table="X",
                dbt_sql="SELECT \"Id\" AS \"Id\" FROM {{ source('s','X') }}",
                column_notes=[],
            )
        )


class FakeResult:
    def __init__(self, overall, models):
        self.overall_status = overall
        self.models = models


class FakeModel:
    def __init__(self, name, status, msg=""):
        self.model_name = name
        self.status = status
        self.error_message = msg


def _mapping(sql):
    return TableMapping(target_table="X", dbt_sql=sql, column_notes=[])


def test_fixer_agent_used_instead_of_writer_on_dbt_failure(tmp_path):
    writer = FakeAgent("writer")
    fixer = FakeAgent("fixer")
    calls = {"n": 0}

    class Api:
        async def run_dbt(self):
            calls["n"] += 1
            if calls["n"] == 1:
                return FakeResult("error", [FakeModel("X", "error", "boom")])
            return FakeResult("success", [FakeModel("X", "success")])

    (tmp_path / "models").mkdir()
    ok = asyncio.run(
        run_dbt_self_correction(
            Api(),
            writer,
            {"X": _mapping("SELECT 1")},
            "ctx",
            dbt_project_path=tmp_path,
            source_tables=[("s", "X")],
            source_schema="s",
            max_passes=3,
            serial=True,
            fixer_agent=fixer,
            validation_gate="none",
        )
    )
    assert ok is True
    assert fixer.calls == 1
    assert writer.calls == 0


def test_static_gate_fixes_before_any_dbt_run(tmp_path):
    fixer = FakeAgent("fixer")
    ran_dbt = {"n": 0}

    class Api:
        async def run_dbt(self):
            ran_dbt["n"] += 1
            return FakeResult("success", [FakeModel("X", "success")])

    (tmp_path / "models").mkdir()
    ok = asyncio.run(
        run_dbt_self_correction(
            Api(),
            fixer,
            {"X": _mapping("SELECT (")},
            "ctx",
            dbt_project_path=tmp_path,
            source_tables=[("s", "X")],
            source_schema="s",
            max_passes=3,
            serial=True,
            validation_gate="static",
        )
    )
    assert ok is True
    assert fixer.calls >= 1
    assert ran_dbt["n"] >= 1
