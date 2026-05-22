"""Trace sink + event types. Streams JSONL to disk."""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, IO
from pydantic import BaseModel

from aidmi_orchestrator.domain import ModelSpec


class TraceEvent(BaseModel):
    timestamp: datetime
    event_type: str


class LlmCallEvent(TraceEvent):
    event_type: Literal["llm_call"] = "llm_call"
    role: str
    model_spec: ModelSpec
    messages: list[dict[str, Any]]
    response: str | dict[str, Any]
    usage: dict[str, Any]
    latency_ms: float


class DbtRunEvent(TraceEvent):
    event_type: Literal["dbt_run"] = "dbt_run"
    transform_result: dict[str, Any]   # serialized aidmi_pipeline.TransformResult
    duration_ms: float


class ToolCallEvent(TraceEvent):
    event_type: Literal["tool_call"] = "tool_call"
    tool_name: str
    args: dict[str, Any]
    result: Any
    latency_ms: float


class StrategyEvent(TraceEvent):
    event_type: Literal["strategy"] = "strategy"
    label: str
    data: dict[str, Any]


_EVENT_CLASSES: dict[str, type[TraceEvent]] = {
    "llm_call": LlmCallEvent,
    "dbt_run": DbtRunEvent,
    "tool_call": ToolCallEvent,
    "strategy": StrategyEvent,
}


class TraceSink:
    """Append-only JSONL writer; one event per line. Streams during execution."""

    def __init__(self, path: Path, mirror_to: IO[str] | None = None):
        self.path = path
        self._mirror_to = mirror_to
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh: IO[str] = open(self.path, "a", encoding="utf-8")

    def record(self, event: TraceEvent) -> None:
        line = event.model_dump_json() + "\n"
        self._fh.write(line)
        self._fh.flush()
        if self._mirror_to is not None:
            self._mirror_to.write(line)
            self._mirror_to.flush()

    def close(self) -> None:
        self._fh.close()

    @staticmethod
    def read_all(path: Path) -> list[TraceEvent]:
        events: list[TraceEvent] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            obj = json.loads(line)
            cls = _EVENT_CLASSES.get(obj.get("event_type"))
            if cls is None:
                continue
            events.append(cls.model_validate(obj))
        return events
