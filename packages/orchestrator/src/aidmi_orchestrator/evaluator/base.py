"""Evaluator Protocol + RunArtifacts + registry."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from aidmi_orchestrator.domain import StrategyResult, TargetSchema
from aidmi_orchestrator.trace import TraceEvent


@dataclass
class FixtureMetadata:
    name: str
    description: str
    reference_dbt_path: Path | None
    applicable_evaluators: list[str]


@dataclass
class RunArtifacts:
    run_id: str
    dbt_project_path: Path
    dlt_pipelines_dir: Path
    staging_db_url: str
    staging_raw_dataset: str
    staging_out_dataset: str
    trace: list[TraceEvent]
    strategy_result: StrategyResult
    target_schema_input: TargetSchema | None
    fixture: FixtureMetadata
    wall_clock_seconds: float
    final_transform_result: Any | None    # aidmi_pipeline.TransformResult or None on crash


@runtime_checkable
class Evaluator(Protocol):
    name: str
    def applies_to(self, artifacts: RunArtifacts) -> bool: ...
    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]: ...


_EVALUATORS: dict[str, type] = {}


def register_evaluator(name: str, cls: type) -> None:
    if name in _EVALUATORS:
        raise ValueError(f"evaluator {name!r} already registered")
    _EVALUATORS[name] = cls


def list_evaluators() -> list[str]:
    return sorted(_EVALUATORS)


def make_evaluator(name: str, **kwargs) -> Evaluator:
    if name not in _EVALUATORS:
        raise ValueError(f"unknown evaluator {name!r}. Registered: {list_evaluators()}")
    return _EVALUATORS[name](**kwargs)
