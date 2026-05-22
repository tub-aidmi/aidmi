"""Sources.yml normalization and write_tools_freeform tool edge cases."""

from __future__ import annotations

import asyncio
from pathlib import Path

import yaml

from aidmi_orchestrator.strategy.base import ensure_sources_yaml_target_schema
from aidmi_orchestrator.strategy.write_tools_freeform.tools import make_run_dbt


BAD_YAML = """version: 2

sources:
  - name: raw_contacts
    tables:
      - name: contacts
"""

EXPECTED_MACRO = "{{ target.schema }}"


def test_ensure_sources_yaml_injects_target_schema_macro(tmp_path: Path) -> None:
    path = tmp_path / "sources.yml"
    path.write_text(BAD_YAML, encoding="utf-8")
    ensure_sources_yaml_target_schema(path)

    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert loaded["sources"][0]["schema"] == EXPECTED_MACRO


def test_ensure_sources_yaml_idempotent_when_present(tmp_path: Path) -> None:
    yaml_text = f"""version: 2

sources:
  - name: x
    schema: '{EXPECTED_MACRO}'
    tables:
      - name: t
"""
    path = tmp_path / "sources.yml"
    path.write_text(yaml_text, encoding="utf-8")
    before = path.read_text(encoding="utf-8")
    ensure_sources_yaml_target_schema(path)
    after = path.read_text(encoding="utf-8")
    assert before == after


def test_make_run_dbt_returns_error_dict_on_failure() -> None:
    class CrashAPI:
        async def run_dbt(self):  # noqa: ANN201
            raise RuntimeError("dbt exploded")

    run_dbt = make_run_dbt(CrashAPI(), max_passes=3)

    async def _invoke() -> dict:
        return await run_dbt()

    out = asyncio.run(_invoke())
    assert out["overall_status"] == "error"
    assert "RuntimeError" in out["error"]
