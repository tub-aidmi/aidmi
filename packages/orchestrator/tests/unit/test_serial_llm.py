"""serial_llm_calls: run_coroutines and serial regeneration."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from aidmi_orchestrator.strategy.base import run_coroutines
from aidmi_orchestrator.strategy.dbt_retry import retry_failing_tables


def test_run_coroutines_serial_executes_in_order() -> None:
    order: list[int] = []

    async def work(n: int) -> int:
        order.append(n)
        return n

    result = asyncio.run(run_coroutines([work(1), work(2), work(3)], serial=True))
    assert result == [1, 2, 3]
    assert order == [1, 2, 3]


def test_run_coroutines_parallel_returns_all() -> None:
    async def work(n: int) -> int:
        return n

    result = asyncio.run(run_coroutines([work(1), work(2), work(3)], serial=False))
    assert sorted(result) == [1, 2, 3]


def test_retry_failing_tables_serial_regenerates_in_order() -> None:
    order: list[str] = []

    async def regenerate(name: str, _err: str) -> None:
        order.append(name)

    fail = SimpleNamespace(
        overall_status="error",
        models=[
            SimpleNamespace(
                model_name="orgs", status="error", error_message="orgs broke"
            ),
            SimpleNamespace(
                model_name="users", status="error", error_message="users broke"
            ),
        ],
    )
    ok = SimpleNamespace(overall_status="success", models=[])
    run_dbt = AsyncMock(side_effect=[fail, ok])

    result = asyncio.run(
        retry_failing_tables(
            run_dbt,
            regenerate,
            max_passes=3,
            serial=True,
        )
    )
    assert result is True
    assert order == ["orgs", "users"]
