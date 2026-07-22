"""live_query_tool mode must give the per-table agents a query_postgres tool."""

from __future__ import annotations

import asyncio

from pydantic_ai.models.test import TestModel

from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.strategy.structured_per_table.strategy import (
    StructuredPerTable,
    StructuredPerTableConfig,
)

from .test_structured_common import MAPPING_ARGS, fake_api


def _spec() -> ModelSpec:
    return ModelSpec(provider="litellm", model_name="m")


def test_live_query_tool_mode_calls_query_postgres(tmp_path) -> None:
    model = TestModel(custom_output_args=MAPPING_ARGS)
    api = fake_api(tmp_path, make_llm=lambda spec, role: model)
    strategy = StructuredPerTable(
        StructuredPerTableConfig(
            writer_model=_spec(),
            context_mode="live_query_tool",
        )
    )
    result = asyncio.run(strategy.generate(api))
    assert result.self_reported_status == "complete"
    api.query_postgres.assert_called()
    assert (tmp_path / "models" / "users.sql").exists()


def test_metadata_mode_has_no_query_tool(tmp_path) -> None:
    model = TestModel(custom_output_args=MAPPING_ARGS)
    api = fake_api(tmp_path, make_llm=lambda spec, role: model)
    strategy = StructuredPerTable(
        StructuredPerTableConfig(
            writer_model=_spec(),
            context_mode="metadata_plus_samples",
        )
    )
    asyncio.run(strategy.generate(api))
    api.query_postgres.assert_not_called()
