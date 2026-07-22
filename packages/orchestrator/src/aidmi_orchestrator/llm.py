"""Provider registry + Model construction + TracedModel wrapper."""

from __future__ import annotations

import asyncio
import dataclasses
import os
import time
from collections.abc import Callable
from typing import Any

from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.messages import ModelMessage, ModelResponse
from pydantic_ai.models import ModelRequestParameters
from pydantic_ai.models.wrapper import WrapperModel
from pydantic_ai.settings import ModelSettings

from aidmi_orchestrator.clock import utc_now
from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.llm_retry import (
    is_retryable_model_http_error,
    resolve_retry_settings,
    retry_delay_seconds,
)
from aidmi_orchestrator.progress import log_message
from aidmi_orchestrator.trace import LlmCallEvent, TraceSink

ProviderFactory = Callable[[ModelSpec], Any]
_PROVIDERS: dict[str, ProviderFactory] = {}


def register_provider(name: str, factory: ProviderFactory) -> None:
    if name in _PROVIDERS:
        raise ValueError(f"provider {name!r} already registered")
    _PROVIDERS[name] = factory


def list_providers() -> list[str]:
    return sorted(_PROVIDERS)


def make_llm(spec: ModelSpec) -> Any:
    if spec.provider not in _PROVIDERS:
        raise ValueError(
            f"unknown provider {spec.provider!r}. Registered: {list_providers()}"
        )
    return _PROVIDERS[spec.provider](spec)


# ---------- Built-in provider factories ----------


def _resolve_api_key(spec: ModelSpec) -> str | None:
    return os.environ[spec.api_key_env] if spec.api_key_env else None


def _google_cloud_factory(spec: ModelSpec):
    from pydantic_ai.models.google import GoogleModel
    from pydantic_ai.providers.google_cloud import GoogleCloudProvider

    api_key = _resolve_api_key(spec)
    extra = spec.extra or {}
    kwargs: dict[str, Any] = {}
    if api_key is not None:
        kwargs["api_key"] = api_key
    if "project" in extra:
        kwargs["project"] = extra["project"]
    if "location" in extra:
        kwargs["location"] = extra["location"]
    if spec.base_url is not None:
        kwargs["base_url"] = spec.base_url
    return GoogleModel(
        spec.model_name,
        provider=GoogleCloudProvider(**kwargs),
    )


def _openai_factory(spec: ModelSpec):
    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.openai import OpenAIProvider

    return OpenAIChatModel(
        spec.model_name,
        provider=OpenAIProvider(base_url=spec.base_url, api_key=_resolve_api_key(spec)),
    )


def _anthropic_factory(spec: ModelSpec):
    from pydantic_ai.models.anthropic import AnthropicModel
    from pydantic_ai.providers.anthropic import AnthropicProvider

    return AnthropicModel(
        spec.model_name,
        provider=AnthropicProvider(api_key=_resolve_api_key(spec)),
    )


register_provider("openai", _openai_factory)
register_provider("openai_compatible", _openai_factory)


def _ollama_base_url(spec: ModelSpec) -> str:
    base = (spec.base_url or "http://localhost:11434").rstrip("/")
    if not base.endswith("/v1"):
        base = f"{base}/v1"
    return base


def _ollama_factory(spec: ModelSpec):
    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.ollama import OllamaProvider

    return OpenAIChatModel(
        spec.model_name,
        provider=OllamaProvider(
            base_url=_ollama_base_url(spec),
        ),
    )


register_provider("ollama", _ollama_factory)
register_provider("anthropic", _anthropic_factory)
register_provider("litellm", _openai_factory)
register_provider("google_cloud", _google_cloud_factory)


# ---------- TracedModel ----------


def _base_usage_dict(response: ModelResponse) -> dict[str, Any]:
    u = response.usage
    if u is None:
        return {
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_read_tokens": 0,
            "cache_write_tokens": 0,
        }
    return {
        "input_tokens": getattr(u, "input_tokens", 0) or 0,
        "output_tokens": getattr(u, "output_tokens", 0) or 0,
        "cache_read_tokens": getattr(u, "cache_read_tokens", 0) or 0,
        "cache_write_tokens": getattr(u, "cache_write_tokens", 0) or 0,
    }


def _usage_dict(response: ModelResponse) -> dict[str, Any]:
    usage = _base_usage_dict(response)
    try:
        u = response.usage
        if u is not None:
            raw_details = getattr(u, "details", None) or {}
            if isinstance(raw_details, dict) and raw_details:
                details: dict[str, int | float] = {}
                for key, value in raw_details.items():
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        details[str(key)] = value
                if details:
                    usage["details"] = details
        vendor_details = (
            getattr(response, "provider_details", None)
            or getattr(response, "vendor_details", None)
            or {}
        )
        if isinstance(vendor_details, dict) and vendor_details:
            vendor: dict[str, str] = {}
            for key in ("traffic_type", "service_tier"):
                value = vendor_details.get(key)
                if isinstance(value, str) and value:
                    vendor[key] = value
            if vendor:
                usage["vendor"] = vendor
    except Exception:
        pass
    return usage


@dataclasses.dataclass(init=False)
class TracedModel(WrapperModel):
    """Wraps a PydanticAI Model via WrapperModel. Every request() call records an LlmCallEvent."""

    _spec: ModelSpec
    _role: str
    _trace: TraceSink

    def __init__(self, inner: Any, spec: ModelSpec, role: str, trace: TraceSink):
        super().__init__(inner)
        self._spec = spec
        self._role = role
        self._trace = trace

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        max_retries, base_seconds, max_seconds = resolve_retry_settings(self._spec)
        scope = getattr(self._trace, "_progress_scope", None)

        for attempt in range(max_retries + 1):
            start = time.perf_counter()
            try:
                response = await self.wrapped.request(
                    messages,
                    model_settings,
                    model_request_parameters,
                )
            except ModelHTTPError as exc:
                if attempt >= max_retries or not is_retryable_model_http_error(exc):
                    raise
                delay = retry_delay_seconds(
                    attempt,
                    base_seconds=base_seconds,
                    max_seconds=max_seconds,
                )
                log_message(
                    f"LLM {self._role} {self._spec.model_name} HTTP {exc.status_code}, "
                    f"retry {attempt + 1}/{max_retries} in {delay:.1f}s",
                    scope=scope,
                )
                await asyncio.sleep(delay)
                continue

            latency_ms = (time.perf_counter() - start) * 1000
            usage_dict = _usage_dict(response)
            if attempt > 0:
                usage_dict["retry_count"] = attempt
            self._trace.record(
                LlmCallEvent(
                    timestamp=utc_now(),
                    role=self._role,
                    model_spec=self._spec,
                    messages=[
                        m.model_dump() if hasattr(m, "model_dump") else {"raw": str(m)}
                        for m in messages
                    ],
                    response=(
                        response.model_dump()
                        if hasattr(response, "model_dump")
                        else str(response)
                    ),
                    usage=usage_dict,
                    latency_ms=latency_ms,
                )
            )
            return response

        raise RuntimeError(
            "unreachable: LLM retry loop exhausted without response or exception"
        )
