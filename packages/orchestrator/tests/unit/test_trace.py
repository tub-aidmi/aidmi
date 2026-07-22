import json
from datetime import datetime

from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.trace import (
    LlmCallEvent,
    StrategyEvent,
    ToolCallEvent,
    TraceSink,
)


def test_trace_sink_writes_jsonl(tmp_path):
    sink = TraceSink(tmp_path / "trace.jsonl")
    sink.record(
        StrategyEvent(
            timestamp=datetime(2026, 5, 17, 12, 0, 0),
            label="hello",
            data={"x": 1},
        )
    )
    sink.record(
        LlmCallEvent(
            timestamp=datetime(2026, 5, 17, 12, 0, 1),
            role="writer",
            model_spec=ModelSpec(provider="openai", model_name="gpt-4o-mini"),
            messages=[{"role": "user", "content": "hi"}],
            response="hello",
            usage={"prompt_tokens": 10, "completion_tokens": 5},
            latency_ms=123.4,
        )
    )
    sink.close()

    lines = (tmp_path / "trace.jsonl").read_text().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["event_type"] == "strategy"
    assert first["label"] == "hello"
    second = json.loads(lines[1])
    assert second["event_type"] == "llm_call"
    assert second["role"] == "writer"


def test_trace_sink_read_all_round_trips(tmp_path):
    sink = TraceSink(tmp_path / "trace.jsonl")
    sink.record(
        ToolCallEvent(
            timestamp=datetime(2026, 5, 17),
            tool_name="write_file",
            args={"path": "x"},
            result="ok",
            latency_ms=1.0,
        )
    )
    sink.close()
    events = TraceSink.read_all(tmp_path / "trace.jsonl")
    assert len(events) == 1
    assert isinstance(events[0], ToolCallEvent)
    assert events[0].tool_name == "write_file"
