"""Normalize dbt sources.yml to point logical sources at the raw extract schema."""

from __future__ import annotations

from pathlib import Path

import yaml


def ensure_sources_yaml_raw_schema(models_dir: Path, raw_schema: str) -> None:
    """Force every logical source block to read from raw_schema (Postgres identifier).

    dbt target.schema resolves to the out schema; sources must remain on raw.
    """
    path = models_dir / "sources.yml"
    if not path.exists():
        return
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
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
        if src.get("schema") != raw_schema:
            src["schema"] = raw_schema
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
    path.write_text(dumped, encoding="utf-8")
