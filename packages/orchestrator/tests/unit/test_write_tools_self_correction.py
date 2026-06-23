"""write_tools_freeform prompt and self-correction safety net."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from aidmi_orchestrator.strategy.write_tools_freeform.prompts import (
    build_initial_user_prompt,
    build_system_prompt,
)
from aidmi_orchestrator.strategy.write_tools_freeform.self_correction import (
    format_dbt_errors,
    run_post_agent_dbt_loop,
)


def test_build_system_prompt_includes_postgres_rules_by_default() -> None:
    prompt = build_system_prompt(enable_self_correction=False)
    assert "TRY_CAST" in prompt
    assert "CASE WHEN col ~ '^\\d+$'" in prompt


def test_build_system_prompt_adds_self_correction_requirements() -> None:
    prompt = build_system_prompt(enable_self_correction=True)
    assert "You MUST call run_dbt()" in prompt
    assert "Do NOT skip run_dbt()" in prompt


def test_build_initial_user_prompt_adds_self_correction_reminder() -> None:
    base = build_initial_user_prompt("ctx", enable_self_correction=False)
    with_sc = build_initial_user_prompt("ctx", enable_self_correction=True)
    assert "Self-correction is ON" in with_sc
    assert "Self-correction is ON" not in base


def test_format_dbt_errors_lists_failed_models() -> None:
    result = SimpleNamespace(
        overall_status="error",
        models=[
            SimpleNamespace(model_name="persons", status="error", error_message="syntax error"),
            SimpleNamespace(model_name="organizations", status="success", error_message=None),
        ],
    )
    text = format_dbt_errors(result)
    assert "overall_status: error" in text
    assert "persons: syntax error" in text


def test_run_post_agent_dbt_loop_returns_true_on_first_success(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "persons.sql").write_text("select 1", encoding="utf-8")

    api = SimpleNamespace(
        source_schema="src_test_raw",
        dbt_project_path=tmp_path,
        run_dbt=AsyncMock(
            return_value=SimpleNamespace(overall_status="success", models=[]),
        ),
    )
    agent = MagicMock()
    agent.run = AsyncMock()

    ok = asyncio.run(
        run_post_agent_dbt_loop(
            api,
            agent,
            MagicMock(),
            max_passes=3,
        )
    )

    assert ok is True
    api.run_dbt.assert_awaited_once()
    agent.run.assert_not_awaited()


def test_run_post_agent_dbt_loop_reprompts_agent_then_succeeds(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "persons.sql").write_text("bad sql", encoding="utf-8")

    fail = SimpleNamespace(
        overall_status="error",
        models=[
            SimpleNamespace(
                model_name="persons",
                status="error",
                error_message="syntax error at TRY_CAST",
            ),
        ],
    )
    success = SimpleNamespace(overall_status="success", models=[])

    api = SimpleNamespace(
        source_schema="src_test_raw",
        dbt_project_path=tmp_path,
        run_dbt=AsyncMock(side_effect=[fail, success]),
    )
    agent = MagicMock()
    agent.run = AsyncMock()

    ok = asyncio.run(
        run_post_agent_dbt_loop(
            api,
            agent,
            MagicMock(),
            max_passes=3,
        )
    )

    assert ok is True
    assert api.run_dbt.await_count == 2
    agent.run.assert_awaited_once()
    prompt = agent.run.await_args.args[0]
    assert "TRY_CAST" in prompt or "syntax error at TRY_CAST" in prompt


def test_run_post_agent_dbt_loop_returns_false_when_passes_exhausted(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "persons.sql").write_text("bad sql", encoding="utf-8")

    fail = SimpleNamespace(
        overall_status="error",
        models=[
            SimpleNamespace(
                model_name="persons",
                status="error",
                error_message="still broken",
            ),
        ],
    )

    api = SimpleNamespace(
        source_schema="src_test_raw",
        dbt_project_path=tmp_path,
        run_dbt=AsyncMock(return_value=fail),
    )
    agent = MagicMock()
    agent.run = AsyncMock()

    ok = asyncio.run(
        run_post_agent_dbt_loop(
            api,
            agent,
            MagicMock(),
            max_passes=2,
        )
    )

    assert ok is False
    assert api.run_dbt.await_count == 2
    assert agent.run.await_count == 1
