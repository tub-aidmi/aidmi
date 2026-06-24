"""retry_failing_tables: dbt-feedback loop for structured strategies."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from pydantic_ai.models.test import TestModel

from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.strategy.dbt_retry import retry_failing_tables
from aidmi_orchestrator.strategy.structured_per_table.strategy import (
    StructuredPerTable, StructuredPerTableConfig,
)

from .test_structured_common import MAPPING_ARGS, fake_api


def _fail(*names: str) -> SimpleNamespace:
    return SimpleNamespace(
        overall_status="error",
        models=[SimpleNamespace(model_name=n, status="error", error_message=f"{n} broke") for n in names],
    )


_OK = SimpleNamespace(overall_status="success", models=[])


def test_success_on_first_run_does_not_regenerate() -> None:
    run_dbt = AsyncMock(return_value=_OK)
    regenerate = AsyncMock()
    ok = asyncio.run(retry_failing_tables(run_dbt, regenerate, max_passes=3))
    assert ok is True
    run_dbt.assert_awaited_once()
    regenerate.assert_not_awaited()


def test_regenerates_only_failing_tables_then_succeeds() -> None:
    run_dbt = AsyncMock(side_effect=[_fail("users"), _OK])
    regenerate = AsyncMock()
    ok = asyncio.run(retry_failing_tables(run_dbt, regenerate, max_passes=3))
    assert ok is True
    assert run_dbt.await_count == 2
    regenerate.assert_awaited_once_with("users", "users broke")


def test_gives_up_after_max_passes() -> None:
    run_dbt = AsyncMock(return_value=_fail("users", "orgs"))
    regenerate = AsyncMock()
    ok = asyncio.run(retry_failing_tables(run_dbt, regenerate, max_passes=2))
    assert ok is False
    assert run_dbt.await_count == 2
    assert regenerate.await_count == 2  # one pass of regeneration, two failing tables


def test_dbt_crash_counts_as_failure_without_regeneration_detail() -> None:
    run_dbt = AsyncMock(side_effect=RuntimeError("boom"))
    regenerate = AsyncMock()
    ok = asyncio.run(retry_failing_tables(run_dbt, regenerate, max_passes=1))
    assert ok is False
    regenerate.assert_not_awaited()


def test_failure_without_named_models_does_not_spin() -> None:
    bad = SimpleNamespace(overall_status="partial", models=[])
    run_dbt = AsyncMock(return_value=bad)
    regenerate = AsyncMock()
    ok = asyncio.run(retry_failing_tables(run_dbt, regenerate, max_passes=3))
    assert ok is False
    run_dbt.assert_awaited_once()
    regenerate.assert_not_awaited()


def test_regenerate_crash_is_non_fatal() -> None:
    run_dbt = AsyncMock(return_value=_fail("users"))
    regenerate = AsyncMock(side_effect=RuntimeError("llm down"))
    ok = asyncio.run(retry_failing_tables(run_dbt, regenerate, max_passes=3))
    assert ok is False
    run_dbt.assert_awaited_once()


def test_strategy_reports_partial_when_dbt_never_succeeds(tmp_path) -> None:
    model = TestModel(custom_output_args=MAPPING_ARGS)
    api = fake_api(tmp_path, make_llm=lambda spec, role: model)
    api.run_dbt = AsyncMock(return_value=_fail("users"))
    strategy = StructuredPerTable(StructuredPerTableConfig(
        writer_model=ModelSpec(provider="litellm", model_name="m"),
        enable_self_correction=True,
        max_self_correction_passes=2,
    ))
    result = asyncio.run(strategy.generate(api))
    assert result.self_reported_status == "partial"
    assert api.run_dbt.await_count == 2


def test_strategy_complete_when_self_correction_off(tmp_path) -> None:
    model = TestModel(custom_output_args=MAPPING_ARGS)
    api = fake_api(tmp_path, make_llm=lambda spec, role: model)
    api.run_dbt = AsyncMock()
    strategy = StructuredPerTable(StructuredPerTableConfig(
        writer_model=ModelSpec(provider="litellm", model_name="m"),
    ))
    result = asyncio.run(strategy.generate(api))
    assert result.self_reported_status == "complete"
    api.run_dbt.assert_not_awaited()
