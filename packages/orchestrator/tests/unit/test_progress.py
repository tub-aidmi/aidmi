from datetime import datetime

from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.progress import log_message
from aidmi_orchestrator.trace import LlmCallEvent, format_trace_progress


def test_format_trace_progress_llm_call():
    event = LlmCallEvent(
        timestamp=datetime.utcnow(),
        role="writer",
        model_spec=ModelSpec(provider="openai", model_name="gpt-4o-mini"),
        messages=[],
        response="ok",
        usage={"input_tokens": 10, "output_tokens": 5},
        latency_ms=123.4,
    )
    assert "LLM writer gpt-4o-mini" in format_trace_progress(event)
    assert "in=10 out=5" in format_trace_progress(event)


def test_format_trace_progress_includes_thoughts():
    event = LlmCallEvent(
        timestamp=datetime.utcnow(),
        role="writer",
        model_spec=ModelSpec(provider="google_cloud", model_name="gemini-2.5-flash"),
        messages=[],
        response="ok",
        usage={
            "input_tokens": 10,
            "output_tokens": 5,
            "details": {"thoughts_tokens": 42},
        },
        latency_ms=123.4,
    )
    summary = format_trace_progress(event)
    assert "thoughts=42" in summary


def test_log_message_includes_scope(capsys):
    log_message("hello", scope="sweep")
    err = capsys.readouterr().err
    assert "[sweep] hello" in err
    assert err.strip().split(" ", 1)[0].count("-") == 2
