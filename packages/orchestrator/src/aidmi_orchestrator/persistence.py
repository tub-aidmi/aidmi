"""File writers for run artifacts. No business logic."""
from __future__ import annotations
import shutil
from pathlib import Path
from aidmi_orchestrator.domain import (
    StrategyResult, MappingManifest, BenchmarkResult,
)


_DBT_PROJECT_TEMPLATE = """\
name: aidmi_generated
version: '1.0.0'
config-version: 2
profile: aidmi_generated
model-paths: ["models"]
target-path: target
clean-targets: ["target", "dbt_packages"]
models:
  aidmi_generated:
    +materialized: table
"""


def scaffold_dbt_project(dbt_project_path: Path) -> None:
    dbt_project_path.mkdir(parents=True, exist_ok=True)
    (dbt_project_path / "dbt_project.yml").write_text(_DBT_PROJECT_TEMPLATE, encoding="utf-8")
    (dbt_project_path / "models").mkdir(exist_ok=True)


def write_strategy_result(run_dir: Path, result: StrategyResult) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "strategy_result.json").write_text(
        result.model_dump_json(indent=2), encoding="utf-8"
    )


def write_mapping_manifest(run_dir: Path, manifest: MappingManifest | None) -> None:
    if manifest is None:
        return
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "mapping_manifest.json").write_text(
        manifest.model_dump_json(indent=2), encoding="utf-8"
    )


def write_benchmark_result(run_dir: Path, result: BenchmarkResult) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "result.json").write_text(
        result.model_dump_json(indent=2), encoding="utf-8"
    )


_DBT_ARCHIVE_IGNORE = shutil.ignore_patterns("target", "dbt_packages", "logs")


def archive_run_dbt(run_dir: Path, dest_dir: Path) -> bool:
    """Copy source dbt project from a run into dest_dir/dbt_project/. Returns False if missing."""
    src = run_dir / "dbt_project"
    if not src.is_dir():
        return False
    dest = dest_dir / "dbt_project"
    if dest.exists():
        return True
    dest_dir.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src, dest, ignore=_DBT_ARCHIVE_IGNORE)
    return True
