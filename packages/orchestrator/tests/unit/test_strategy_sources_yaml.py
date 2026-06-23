"""Sources.yml normalization and write_tools_freeform tool edge cases."""

from __future__ import annotations

import asyncio
from pathlib import Path

import yaml

from aidmi_orchestrator.strategy.base import (
    ensure_sources_yaml_raw_schema,
    normalize_source_refs,
    write_proposal,
)
from aidmi_orchestrator.strategy.write_tools_freeform.tools import make_run_dbt


BAD_YAML = """version: 2

sources:
  - name: raw_contacts
    tables:
      - name: contacts
"""

RAW_SCHEMA = "src_test_raw"


def test_ensure_sources_yaml_injects_raw_schema(tmp_path: Path) -> None:
    path = tmp_path / "sources.yml"
    path.write_text(BAD_YAML, encoding="utf-8")
    ensure_sources_yaml_raw_schema(tmp_path, RAW_SCHEMA)

    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert loaded["sources"][0]["schema"] == RAW_SCHEMA


def test_ensure_sources_yaml_idempotent_when_present(tmp_path: Path) -> None:
    yaml_text = f"""version: 2

sources:
  - name: x
    schema: "{RAW_SCHEMA}"
    tables:
      - name: t
"""
    path = tmp_path / "sources.yml"
    path.write_text(yaml_text, encoding="utf-8")
    before = path.read_text(encoding="utf-8")
    ensure_sources_yaml_raw_schema(tmp_path, RAW_SCHEMA)
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


def test_normalize_source_refs_rewrites_wrong_slug() -> None:
    sql = "SELECT 1 FROM {{ source('src_master', 'master_kunden') }}"
    out = normalize_source_refs(
        sql,
        canonical_slug="fixture_master_src",
        known_tables={"master_kunden"},
    )
    assert "{{ source('fixture_master_src', 'master_kunden') }}" in out
    assert "src_master" not in out


def test_normalize_source_refs_leaves_correct_slug() -> None:
    sql = "SELECT 1 FROM {{ source('fixture_master_src', 'master_kunden') }}"
    assert normalize_source_refs(
        sql,
        canonical_slug="fixture_master_src",
        known_tables={"master_kunden"},
    ) == sql


def test_write_proposal_normalizes_source_refs(tmp_path: Path) -> None:
    write_proposal(
        tmp_path,
        {"Account": "SELECT 1 FROM {{ source('src_master', 'master_kunden') }}"},
        [("fixture_master_src", "master_kunden")],
        "fixture_master_src",
    )
    written = (tmp_path / "models" / "Account.sql").read_text(encoding="utf-8")
    assert "{{ source('fixture_master_src', 'master_kunden') }}" in written
