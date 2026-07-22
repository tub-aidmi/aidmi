"""dbt self-correction retry loop."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from aidmi_orchestrator.strategy.dbt_retry import (
    extract_failing_models,
    retry_failing_tables,
)


def test_extract_failing_models_uses_bare_table_names() -> None:
    result = SimpleNamespace(
        models=[
            SimpleNamespace(
                model_name="Installed_Asset__c", status="error", error_message="syntax"
            ),
            SimpleNamespace(model_name="Account", status="success", error_message=None),
        ]
    )
    assert extract_failing_models(result) == [("Installed_Asset__c", "syntax")]


def test_retry_regenerates_schema_prefixed_failures() -> None:
    run_dbt = AsyncMock(
        side_effect=[
            SimpleNamespace(
                overall_status="error",
                models=[
                    SimpleNamespace(
                        model_name="out_schema.Installed_Asset__c",
                        status="error",
                        error_message="syntax error",
                    ),
                ],
            ),
            SimpleNamespace(overall_status="success", models=[]),
        ]
    )
    regenerate = AsyncMock()
    ok = asyncio.run(retry_failing_tables(run_dbt, regenerate, max_passes=3))
    assert ok is True
    regenerate.assert_awaited_once_with("Installed_Asset__c", "syntax error")


def test_retry_uses_all_tables_when_failure_details_missing() -> None:
    run_dbt = AsyncMock(
        side_effect=[
            SimpleNamespace(overall_status="error", models=[]),
            SimpleNamespace(overall_status="success", models=[]),
        ]
    )
    regenerate = AsyncMock()
    ok = asyncio.run(
        retry_failing_tables(
            run_dbt,
            regenerate,
            max_passes=3,
            all_table_names=["Account", "Contact"],
        )
    )
    assert ok is True
    assert regenerate.await_count == 2
    regenerate.assert_any_await("Account", "overall_status: error")
    regenerate.assert_any_await("Contact", "overall_status: error")


def test_retry_all_tables_fallback_runs_serial_even_when_parallel() -> None:
    run_dbt = AsyncMock(
        side_effect=[
            SimpleNamespace(overall_status="error", models=[]),
            SimpleNamespace(overall_status="success", models=[]),
        ]
    )
    regenerate = AsyncMock()
    call_order: list[str] = []

    async def track(name: str, err: str) -> None:
        call_order.append(name)

    regenerate.side_effect = track
    ok = asyncio.run(
        retry_failing_tables(
            run_dbt,
            regenerate,
            max_passes=3,
            serial=False,
            all_table_names=["Account", "Contact"],
        )
    )
    assert ok is True
    assert call_order == ["Account", "Contact"]
