"""Strategy Protocol + registry + shared helpers."""
from __future__ import annotations
import asyncio
import re
from pathlib import Path
from typing import Any, Awaitable, Protocol, TypeVar, runtime_checkable

T = TypeVar("T")

from pydantic import BaseModel

from aidmi_pipeline.sources_yaml import ensure_sources_yaml_raw_schema

from aidmi_orchestrator.strategy.sql_sanitize import sanitize_dbt_sql

from aidmi_orchestrator.domain import (
    SourceSummary, TargetSchema, MappingManifest, TableMappingNote,
    ColumnNote, StrategyResult,
)
from aidmi_orchestrator.strategy.guidelines.compose import context_transformation_section


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


async def run_coroutines(coros: list[Awaitable[T]], *, serial: bool) -> list[T]:
    if serial:
        return [await c for c in coros]
    return list(await asyncio.gather(*coros))


async def run_named_coroutines(
    items: list[tuple[str, Awaitable[T]]],
    *,
    serial: bool,
) -> dict[str, T]:
    """Run coroutines keyed by name; raise RuntimeError listing all failures."""
    if not items:
        return {}
    names = [name for name, _ in items]
    coros = [coro for _, coro in items]
    if serial:
        results: list[T | BaseException] = []
        for coro in coros:
            try:
                results.append(await coro)
            except BaseException as exc:
                results.append(exc)
    else:
        results = list(await asyncio.gather(*coros, return_exceptions=True))
    failures = [
        f"{name}: {result!r}"
        for name, result in zip(names, results)
        if isinstance(result, BaseException)
    ]
    if failures:
        raise RuntimeError("parallel task(s) failed: " + "; ".join(failures))
    return {name: result for name, result in zip(names, results) if not isinstance(result, BaseException)}


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
        table_names = [t.name for t in target_schema.tables]
        lines.append(
            f"\nYou MUST produce exactly one dbt model per target table "
            f"({len(table_names)} total): {', '.join(table_names)}."
        )
    else:
        lines.append("\n# Target schema\n(no constraint supplied — design one.)")

    if source_summary.tables:
        slug = source_summary.tables[0].db_schema
        hint = (
            f"dbt source slug: `{slug}` — use this exact string as the first argument to "
            f"`source()` in dbt models.\n"
        )
        if mode == "live_query_tool":
            t = source_summary.tables[0]
            hint += (
                f"`query_postgres` uses plain PostgreSQL only (no `{{{{ source(...) }}}}`): "
                f'e.g. `SELECT * FROM "{t.db_schema}"."{t.name}" LIMIT 10`.\n'
            )
        lines.insert(1, hint)

    lines.append("\n# Transformation guidelines\n")
    lines.append(context_transformation_section())

    return "\n".join(lines)


_SOURCE_CALL_RE = re.compile(
    r"\{\{\s*source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
)


def normalize_source_refs(
    sql: str,
    *,
    canonical_slug: str,
    known_tables: set[str],
) -> str:
    """Rewrite source() calls that use the wrong logical slug but a known table name."""
    def repl(match: re.Match[str]) -> str:
        slug, table = match.group(1), match.group(2)
        if table in known_tables and slug != canonical_slug:
            return f"{{{{ source('{canonical_slug}', '{table}') }}}}"
        return match.group(0)

    return _SOURCE_CALL_RE.sub(repl, sql)


def discover_model_sql_files(dbt_project_path: Path) -> list[Path]:
    """Find model SQL files; prefer top-level models/ over nested paths."""
    primary = dbt_project_path / "models"
    if primary.is_dir():
        primary_files = sorted(primary.glob("*.sql"))
        if primary_files:
            return primary_files

    candidates: dict[str, Path] = {}
    for path in dbt_project_path.glob("**/models/*.sql"):
        stem = path.stem
        depth = len(path.relative_to(dbt_project_path).parts)
        existing = candidates.get(stem)
        if existing is None or depth < len(existing.relative_to(dbt_project_path).parts):
            candidates[stem] = path
    return sorted(candidates.values(), key=lambda p: p.stem)


def write_proposal(
    dbt_project_path: Path,
    sql_by_table: dict[str, str],
    source_tables: list[tuple[str, str]],  # (schema, name) pairs
    raw_schema: str,
) -> None:
    """Write SQL files + sources.yml. Strategies that produce a structured
    proposal can call this helper; tool-based strategies write files directly.

    Logical source names come from discovery / mapping (`source_slug` in dbt );
    ``raw_schema`` is the Postgres schema where extract landed (`src_<run>_raw`).
    """
    models_dir = dbt_project_path / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    tables_by_schema: dict[str, set[str]] = {}
    for schema, tname in source_tables:
        tables_by_schema.setdefault(schema, set()).add(tname)
    for target_table, sql in sql_by_table.items():
        normalized = sanitize_dbt_sql(sql)
        for schema, tables in tables_by_schema.items():
            normalized = normalize_source_refs(
                normalized, canonical_slug=schema, known_tables=tables,
            )
        (models_dir / f"{target_table}.sql").write_text(normalized, encoding="utf-8")

    src_yaml_lines = ["version: 2", "sources:"]
    schemas = {schema for schema, _ in source_tables}
    for schema in sorted(schemas):
        src_yaml_lines.append(f"  - name: {schema}")
        src_yaml_lines.append(f'    schema: "{raw_schema}"')
        src_yaml_lines.append("    tables:")
        for s, tname in source_tables:
            if s == schema:
                src_yaml_lines.append(f"      - name: {tname}")
    (models_dir / "sources.yml").write_text("\n".join(src_yaml_lines) + "\n", encoding="utf-8")
    ensure_sources_yaml_raw_schema(models_dir, raw_schema)


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
    "run_coroutines",
    "run_named_coroutines",
    "build_context_prompt",
    "normalize_source_refs",
    "write_proposal",
    "ensure_sources_yaml_raw_schema",
    "build_manifest_from_notes",
    "ColumnNote",
    "TableMappingNote",
]

