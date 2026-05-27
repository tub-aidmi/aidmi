"""Provider registry + Model construction + TracedModel wrapper."""
from __future__ import annotations
import dataclasses
import os
import time
from typing import Any, Callable
from datetime import datetime

from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.trace import TraceSink, LlmCallEvent


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


# ---------- TracedModel ----------

from pydantic_ai.models.wrapper import WrapperModel
from pydantic_ai.models import ModelRequestParameters
from pydantic_ai.messages import ModelResponse, ModelMessage
from pydantic_ai.settings import ModelSettings


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
        start = time.perf_counter()
        response = await self.wrapped.request(messages, model_settings, model_request_parameters)
        latency_ms = (time.perf_counter() - start) * 1000
        u = response.usage
        usage_dict = {
            "input_tokens": u.input_tokens,
            "output_tokens": u.output_tokens,
            "cache_read_tokens": u.cache_read_tokens,
            "cache_write_tokens": u.cache_write_tokens,
        }
        self._trace.record(LlmCallEvent(
            timestamp=datetime.utcnow(),
            role=self._role,
            model_spec=self._spec,
            messages=[m.model_dump() if hasattr(m, "model_dump") else {"raw": str(m)} for m in messages],
            response=(response.model_dump() if hasattr(response, "model_dump") else str(response)),
            usage=usage_dict,
            latency_ms=latency_ms,
        ))
        return response
