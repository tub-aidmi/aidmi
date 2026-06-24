"""File writers for run artifacts and campaign recording."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import yaml

from aidmi_orchestrator.campaign import bundle_dir_for_run
from aidmi_orchestrator.domain import (
    BenchmarkResult,
    MappingManifest,
    StrategyResult,
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

_DBT_ARCHIVE_IGNORE = shutil.ignore_patterns("target", "dbt_packages", "logs")


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


def copy_dbt_project(src_run_dir: Path, dest_bundle_dir: Path) -> bool:
    src = src_run_dir / "dbt_project"
    if not src.is_dir():
        return False
    dest = dest_bundle_dir / "dbt_project"
    if dest.exists():
        shutil.rmtree(dest)
    dest_bundle_dir.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src, dest, ignore=_DBT_ARCHIVE_IGNORE)
    return True


def write_strategy_spec_copy(
    bundle_dir: Path,
    *,
    spec_path: Path | None = None,
    cell_spec: dict | None = None,
) -> None:
    bundle_dir.mkdir(parents=True, exist_ok=True)
    dest = bundle_dir / "strategy_spec.yaml"
    if spec_path is not None:
        dest.write_text(spec_path.read_text(encoding="utf-8"), encoding="utf-8")
    elif cell_spec is not None:
        dest.write_text(yaml.safe_dump(cell_spec, sort_keys=False), encoding="utf-8")


def record_run(
    campaign_dir: Path,
    result: BenchmarkResult,
    workspace_run_dir: Path,
    *,
    strategy_spec_path: Path | None = None,
    cell_spec: dict | None = None,
    archive_dbt: bool = True,
) -> Path:
    """Write run bundle under campaign and append to results.jsonl."""
    bundle = bundle_dir_for_run(campaign_dir, result.run_id, result.rep_index)
    bundle.mkdir(parents=True, exist_ok=True)

    write_benchmark_result(bundle, result)

    if strategy_spec_path is not None:
        write_strategy_spec_copy(bundle, spec_path=strategy_spec_path)
    elif cell_spec is not None:
        write_strategy_spec_copy(bundle, cell_spec=cell_spec)

    trace_src = workspace_run_dir / "trace.jsonl"
    if trace_src.is_file():
        shutil.copy2(trace_src, bundle / "trace.jsonl")

    if archive_dbt:
        copy_dbt_project(workspace_run_dir, bundle)

    results_path = campaign_dir / "results.jsonl"
    with open(results_path, "a", encoding="utf-8") as fh:
        fh.write(result.model_dump_json() + "\n")

    return bundle


def load_result_json(bundle_or_legacy_dir: Path) -> BenchmarkResult:
    path = bundle_or_legacy_dir / "result.json"
    if not path.is_file():
        parent = bundle_or_legacy_dir.parent
        if (parent / "runs" / bundle_or_legacy_dir.name / "result.json").is_file():
            path = parent / "runs" / bundle_or_legacy_dir.name / "result.json"
        elif (parent / "results" / "dbt" / bundle_or_legacy_dir.name / "result.json").is_file():
            path = parent / "results" / "dbt" / bundle_or_legacy_dir.name / "result.json"
        else:
            raise FileNotFoundError(f"result.json not found under {bundle_or_legacy_dir}")
    return BenchmarkResult.model_validate_json(path.read_text(encoding="utf-8"))


def load_result_from_campaign(campaign_dir: Path, run_id: str, rep_index: int = 0) -> BenchmarkResult:
    bundle = bundle_dir_for_run(campaign_dir, run_id, rep_index)
    if (bundle / "result.json").is_file():
        return load_result_json(bundle)

    results_path = campaign_dir / "results.jsonl"
    if not results_path.is_file():
        legacy = campaign_dir / "results" / "results.jsonl"
        if legacy.is_file():
            results_path = legacy

    if results_path.is_file():
        for line in results_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("run_id") == run_id and row.get("rep_index", 0) == rep_index:
                return BenchmarkResult.model_validate(row)

    raise FileNotFoundError(f"no result for run_id={run_id!r} rep={rep_index} in {campaign_dir}")
