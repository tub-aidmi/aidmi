"""Strategy Protocol + registry + shared helpers."""
from __future__ import annotations
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import yaml
from pydantic import BaseModel

from aidmi_orchestrator.domain import (
    SourceSummary, TargetSchema, MappingManifest, TableMappingNote,
    ColumnNote, StrategyResult,
)


@runtime_checkable
class Strategy(Protocol):
    name: str
    config: BaseModel

    async def generate(self, api: Any) -> StrategyResult: ...


_STRATEGIES: dict[str, tuple[type, type[BaseModel] | None]] = {}


def register_strategy(name: str, cls: type, config_cls: type[BaseModel] | None = None) -> None:
    if name in _STRATEGIES:
        raise ValueError(f"strategy {name!r} already registered")
    _STRATEGIES[name] = (cls, config_cls)


def list_strategies() -> list[str]:
    return sorted(_STRATEGIES)


def make_strategy(name: str, config_dict: dict[str, Any] | None = None) -> Strategy:
    if name not in _STRATEGIES:
        raise ValueError(f"unknown strategy {name!r}. Registered: {list_strategies()}")
    cls, config_cls = _STRATEGIES[name]
    cfg = config_cls(**(config_dict or {})) if config_cls is not None else None
    return cls(cfg) if cfg is not None else cls()


# ---------- Shared helpers (used by per-table & write-tools strategies) ----------

def build_context_prompt(
    source_summary: SourceSummary,
    target_schema: TargetSchema | None,
    mode: str,
    samples_per_table: int = 3,
) -> str:
    """Build the source/target description text shown to an LLM.

    `mode` ∈ {"metadata_only", "metadata_plus_samples", "live_query_tool"}.
    For "live_query_tool" mode the prompt omits samples and adds a note that
    the agent should use the query_postgres tool when it needs row data.
    """
    lines: list[str] = ["# Source database\n"]
    for t in source_summary.tables:
        lines.append(f"\n## {t.db_schema}.{t.name}  ({t.row_count} rows)\n")
        lines.append("Columns:")
        for c in t.columns:
            null = "NULL" if c.nullable else "NOT NULL"
            lines.append(f"- `{c.name}` {c.sql_type} {null}")
        if mode == "metadata_plus_samples" and t.sample_rows:
            lines.append("\nSample rows:")
            for row in t.sample_rows[:samples_per_table]:
                lines.append(f"- {row}")
        elif mode == "live_query_tool":
            lines.append(
                "\n(Use the `query_postgres(sql)` tool if you need to inspect data.)"
            )

    if target_schema is not None:
        lines.append("\n# Target schema\n")
        for t in target_schema.tables:
            lines.append(f"\n## {t.name}")
            if t.description:
                lines.append(f"_{t.description}_")
            lines.append("Columns:")
            for c in t.columns:
                bits = [f"`{c.name}`", c.sql_type]
                if not c.nullable:
                    bits.append("NOT NULL")
                if c.enum_values:
                    bits.append(f"in ({', '.join(c.enum_values)})")
                if c.description:
                    bits.append(f"— {c.description}")
                lines.append("- " + " ".join(bits))
    else:
        lines.append("\n# Target schema\n(no constraint supplied — design one.)")

    return "\n".join(lines)


_SOURCES_TARGET_SCHEMA = "{{ target.schema }}"


def ensure_sources_yaml_target_schema(sources_yaml_path: Path) -> None:
    """Ensure each dbt `sources:` entry has `schema: "{{ target.schema }}"`.

    Needed so physical tables resolve into the staging schema dlt configures for
    the run; omitting schema makes dbt use the logical source name as schema.
    """
    if not sources_yaml_path.exists():
        return
    try:
        data = yaml.safe_load(sources_yaml_path.read_text(encoding="utf-8"))
    except yaml.YAMLError:
        return
    if not isinstance(data, dict):
        return
    sources = data.get("sources")
    if not isinstance(sources, list):
        return
    changed = False
    for src in sources:
        if not isinstance(src, dict):
            continue
        if src.get("schema") != _SOURCES_TARGET_SCHEMA:
            src["schema"] = _SOURCES_TARGET_SCHEMA
            changed = True
    if not changed:
        return
    if "version" not in data:
        data["version"] = 2
    dumped = yaml.safe_dump(
        data,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
    )
    sources_yaml_path.write_text(dumped, encoding="utf-8")


def write_proposal(
    dbt_project_path: Path,
    sql_by_table: dict[str, str],
    source_tables: list[tuple[str, str]],  # (schema, name) pairs
) -> None:
    """Write SQL files + sources.yml. Strategies that produce a structured
    proposal can call this helper; tool-based strategies write files directly.
    """
    models_dir = dbt_project_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    for target_table, sql in sql_by_table.items():
        (models_dir / f"{target_table}.sql").write_text(sql, encoding="utf-8")

    src_yaml_lines = ["version: 2", "sources:"]
    schemas = {schema for schema, _ in source_tables}
    for schema in sorted(schemas):
        src_yaml_lines.append(f"  - name: {schema}")
        src_yaml_lines.append('    schema: "{{ target.schema }}"')
        src_yaml_lines.append("    tables:")
        for s, tname in source_tables:
            if s == schema:
                src_yaml_lines.append(f"      - name: {tname}")
    (models_dir / "sources.yml").write_text("\n".join(src_yaml_lines) + "\n", encoding="utf-8")


def build_manifest_from_notes(
    notes_by_table: dict[str, TableMappingNote],
    strategy_name: str,
    strategy_config: dict[str, Any],
) -> MappingManifest:
    return MappingManifest(
        tables=list(notes_by_table.values()),
        strategy_name=strategy_name,
        strategy_config=strategy_config,
    )


__all__ = [
    "Strategy",
    "register_strategy",
    "list_strategies",
    "make_strategy",
    "build_context_prompt",
    "write_proposal",
    "ensure_sources_yaml_target_schema",
    "build_manifest_from_notes",
    "ColumnNote",
    "TableMappingNote",
]
