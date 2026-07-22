import asyncio

from pydantic_ai.exceptions import ModelHTTPError

from aidmi_orchestrator.strategy.structured_common import (
    TableMapping,
    generate_table_mapping_safe,
    resolve_structured_status,
)


class RaisingAgent:
    async def run(self, prompt, **kw):
        raise ModelHTTPError(
            status_code=400, model_name="m", body={"message": "budget_exceeded"}
        )


def test_placeholder_marks_generation_failed():
    result = asyncio.run(generate_table_mapping_safe(RaisingAgent(), "Account", "ctx"))
    assert result.generation_failed is True


def test_normal_mapping_generation_failed_false():
    m = TableMapping(target_table="X", dbt_sql="SELECT 1", column_notes=[])
    assert m.generation_failed is False


def test_resolve_structured_status_all_failed_errored():
    mappings = [
        TableMapping(
            target_table="A",
            dbt_sql="-- failed",
            column_notes=[],
            generation_failed=True,
        ),
        TableMapping(
            target_table="B",
            dbt_sql="-- failed",
            column_notes=[],
            generation_failed=True,
        ),
    ]
    assert resolve_structured_status(mappings, dbt_ok=False) == "errored"


def test_resolve_structured_status_all_failed_dbt_ok_still_errored():
    mappings = [
        TableMapping(
            target_table="A",
            dbt_sql="-- failed",
            column_notes=[],
            generation_failed=True,
        ),
    ]
    assert resolve_structured_status(mappings, dbt_ok=True) == "errored"


def test_resolve_structured_status_partial_failure_dbt_ok_complete():
    mappings = [
        TableMapping(
            target_table="A",
            dbt_sql="SELECT 1",
            column_notes=[],
            generation_failed=False,
        ),
        TableMapping(
            target_table="B",
            dbt_sql="-- failed",
            column_notes=[],
            generation_failed=True,
        ),
    ]
    assert resolve_structured_status(mappings, dbt_ok=True) == "complete"


def test_resolve_structured_status_partial_failure_dbt_fail_partial():
    mappings = [
        TableMapping(
            target_table="A",
            dbt_sql="SELECT 1",
            column_notes=[],
            generation_failed=False,
        ),
        TableMapping(
            target_table="B",
            dbt_sql="-- failed",
            column_notes=[],
            generation_failed=True,
        ),
    ]
    assert resolve_structured_status(mappings, dbt_ok=False) == "partial"


def test_resolve_structured_status_no_failures_dbt_ok_complete():
    mappings = [
        TableMapping(target_table="A", dbt_sql="SELECT 1", column_notes=[]),
    ]
    assert resolve_structured_status(mappings, dbt_ok=True) == "complete"


def test_resolve_structured_status_no_failures_dbt_fail_partial():
    mappings = [
        TableMapping(target_table="A", dbt_sql="SELECT 1", column_notes=[]),
    ]
    assert resolve_structured_status(mappings, dbt_ok=False) == "partial"


def test_resolve_structured_status_empty_mappings_dbt_ok_complete():
    assert resolve_structured_status([], dbt_ok=True) == "complete"


def test_resolve_structured_status_empty_mappings_dbt_fail_partial():
    assert resolve_structured_status([], dbt_ok=False) == "partial"
