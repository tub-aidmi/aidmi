import asyncio
from dataclasses import dataclass, field
from unittest.mock import patch

import pytest
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.messages import ModelMessage, ModelResponse
from pydantic_ai.models import Model, ModelRequestParameters
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.usage import RequestUsage

from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.llm import TracedModel, _usage_dict
from aidmi_orchestrator.trace import LlmCallEvent, TraceSink


@dataclass(init=False)
class _StubModel(Model):
    _response: ModelResponse
    _model_name: str = field(default="fake", repr=False)

    def __init__(self, response: ModelResponse):
        super().__init__()
        self._response = response

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def system(self) -> str:
        return "stub"

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        return self._response


@dataclass(init=False)
class _FlakyModel(Model):
    _responses: list[ModelResponse | BaseException]
    _call_count: int = field(default=0, repr=False)
    _model_name: str = field(default="fake", repr=False)

    def __init__(self, responses: list[ModelResponse | BaseException]):
        super().__init__()
        self._responses = responses

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def system(self) -> str:
        return "flaky"

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        outcome = self._responses[min(self._call_count, len(self._responses) - 1)]
        self._call_count += 1
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _response(
    *,
    usage: RequestUsage | None = None,
    provider_details: dict | None = None,
) -> ModelResponse:
    return ModelResponse(
        parts=[],
        usage=usage,
        provider_details=provider_details,
    )


def _inner_model(response: ModelResponse) -> _StubModel | FunctionModel:
    if response.usage is None:
        return _StubModel(response)

    async def _fn(messages, info):
        return response

    return FunctionModel(_fn, model_name="fake")


def test_usage_dict_none_usage():
    out = _usage_dict(_response(usage=None))
    assert out == {
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_read_tokens": 0,
        "cache_write_tokens": 0,
    }


def test_usage_dict_gemini_like_details_and_vendor():
    usage = RequestUsage(
        input_tokens=1000,
        output_tokens=200,
        cache_read_tokens=50,
        details={
            "thoughts_tokens": 42,
            "tool_use_prompt_tokens": 30,
            "text_prompt_tokens": 900,
            "bad": "skip",
        },
    )
    out = _usage_dict(_response(
        usage=usage,
        provider_details={"traffic_type": "ON_DEMAND", "service_tier": "standard"},
    ))
    assert out["input_tokens"] == 1000
    assert out["details"]["thoughts_tokens"] == 42
    assert out["details"]["tool_use_prompt_tokens"] == 30
    assert out["details"]["text_prompt_tokens"] == 900
    assert "bad" not in out["details"]
    assert out["vendor"]["traffic_type"] == "ON_DEMAND"
    assert out["vendor"]["service_tier"] == "standard"


def test_usage_dict_skips_non_numeric_details():
    usage = RequestUsage(input_tokens=1, details={"thoughts_tokens": "bad", "ok": 5})
    out = _usage_dict(_response(usage=usage))
    assert out["details"] == {"ok": 5}


def test_usage_dict_corrupt_details_does_not_raise():
    bad = _response(usage=RequestUsage(input_tokens=1))
    object.__setattr__(bad.usage, "details", "not-a-dict")
    out = _usage_dict(bad)
    assert out["input_tokens"] == 1
    assert "details" not in out


def test_traced_model_records_usage_when_usage_is_none(tmp_path):
    inner = _inner_model(_response(usage=None))
    events: list[LlmCallEvent] = []
    sink = TraceSink(tmp_path / "trace.jsonl")

    class _CapturingSink:
        def record(self, event):
            if isinstance(event, LlmCallEvent):
                events.append(event)
            sink.record(event)

    traced = TracedModel(
        inner,
        ModelSpec(provider="google_cloud", model_name="gemini-2.5-flash"),
        "writer",
        _CapturingSink(),
    )
    response = asyncio.run(traced.request([], None, ModelRequestParameters()))
    sink.close()

    assert response is not None
    assert len(events) == 1
    assert events[0].usage["input_tokens"] == 0


def test_traced_model_records_full_gemini_usage(tmp_path):
    usage = RequestUsage(
        input_tokens=5000,
        output_tokens=800,
        details={"thoughts_tokens": 128, "tool_use_prompt_tokens": 64},
    )
    inner = _inner_model(_response(
        usage=usage,
        provider_details={"traffic_type": "ON_DEMAND"},
    ))
    events: list[LlmCallEvent] = []
    sink = TraceSink(tmp_path / "trace.jsonl")

    class _CapturingSink:
        def record(self, event):
            if isinstance(event, LlmCallEvent):
                events.append(event)
            sink.record(event)

    traced = TracedModel(
        inner,
        ModelSpec(provider="google_cloud", model_name="gemini-2.5-flash"),
        "writer",
        _CapturingSink(),
    )
    asyncio.run(traced.request([], None, ModelRequestParameters()))
    sink.close()

    recorded = events[0].usage
    assert recorded["details"]["thoughts_tokens"] == 128
    assert recorded["vendor"]["traffic_type"] == "ON_DEMAND"


def test_traced_model_retries_transient_http_error(tmp_path):
    ok = _response(usage=RequestUsage(input_tokens=10, output_tokens=5))
    inner = _FlakyModel([
        ModelHTTPError(429, "gemini-2.5-flash", {"status": "RESOURCE_EXHAUSTED"}),
        ok,
    ])
    events: list[LlmCallEvent] = []
    sink = TraceSink(tmp_path / "trace.jsonl")

    class _CapturingSink:
        def record(self, event):
            if isinstance(event, LlmCallEvent):
                events.append(event)
            sink.record(event)

    spec = ModelSpec(
        provider="google_cloud",
        model_name="gemini-2.5-flash",
        extra={"llm_retry": {"max_retries": 2, "base_seconds": 0.01, "max_seconds": 0.02}},
    )
    traced = TracedModel(inner, spec, "writer", _CapturingSink())

    async def _noop_sleep(_seconds: float) -> None:
        return None

    with patch("aidmi_orchestrator.llm.asyncio.sleep", new=_noop_sleep):
        response = asyncio.run(traced.request([], None, ModelRequestParameters()))
    sink.close()

    assert response is not None
    assert inner._call_count == 2
    assert len(events) == 1
    assert events[0].usage["retry_count"] == 1


def test_traced_model_does_not_retry_client_error(tmp_path):
    inner = _FlakyModel([
        ModelHTTPError(400, "gemini-2.5-flash", {"message": "bad request"}),
    ])
    sink = TraceSink(tmp_path / "trace.jsonl")
    traced = TracedModel(
        inner,
        ModelSpec(provider="google_cloud", model_name="gemini-2.5-flash"),
        "writer",
        sink,
    )
    with pytest.raises(ModelHTTPError) as exc_info:
        asyncio.run(traced.request([], None, ModelRequestParameters()))
    sink.close()
    assert exc_info.value.status_code == 400
    assert inner._call_count == 1
