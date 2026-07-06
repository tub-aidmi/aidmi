"""Trace sink + event types. Streams JSONL to disk."""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, IO
from pydantic import BaseModel

from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.progress import log_message


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


def format_trace_progress(event: TraceEvent) -> str | None:
    if isinstance(event, LlmCallEvent):
        usage = event.usage or {}
        summary = (
            f"LLM {event.role} {event.model_spec.model_name} "
            f"({event.latency_ms:.0f}ms, "
            f"in={usage.get('input_tokens', '?')} out={usage.get('output_tokens', '?')})"
        )
        try:
            details = usage.get("details") or {}
            thoughts = details.get("thoughts_tokens", 0) or 0
            if isinstance(thoughts, (int, float)) and thoughts > 0:
                summary += f" thoughts={int(thoughts)}"
        except Exception:
            pass
        return summary
    if isinstance(event, ToolCallEvent):
        return f"tool {event.tool_name} ({event.latency_ms:.0f}ms)"
    if isinstance(event, DbtRunEvent):
        status = (event.transform_result or {}).get("overall_status", "?")
        return f"dbt run {status} ({event.duration_ms:.0f}ms)"
    if isinstance(event, StrategyEvent):
        if event.data:
            extra = ", ".join(f"{k}={v!r}" for k, v in event.data.items())
            return f"{event.label} ({extra})"
        return event.label
    return None


class TraceSink:
    """Append-only JSONL writer; one event per line. Streams during execution."""

    def __init__(
        self,
        path: Path,
        mirror_to: IO[str] | None = None,
        *,
        progress_scope: str | None = None,
    ):
        self.path = path
        self._mirror_to = mirror_to
        self._progress_scope = progress_scope
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh: IO[str] = open(self.path, "a", encoding="utf-8")

    def record(self, event: TraceEvent) -> None:
        line = event.model_dump_json() + "\n"
        self._fh.write(line)
        self._fh.flush()
        if self._mirror_to is not None:
            self._mirror_to.write(line)
            self._mirror_to.flush()
        if self._progress_scope is not None:
            summary = format_trace_progress(event)
            if summary is not None:
                log_message(summary, scope=self._progress_scope)

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
