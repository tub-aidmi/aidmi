"""HTTP retry policy for transient LLM provider errors."""
from __future__ import annotations

import random

from pydantic_ai.exceptions import ModelHTTPError

from aidmi_orchestrator.domain import ModelSpec

RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})

DEFAULT_MAX_RETRIES = 5
DEFAULT_BASE_SECONDS = 2.0
DEFAULT_MAX_SECONDS = 60.0


def resolve_retry_settings(spec: ModelSpec) -> tuple[int, float, float]:
    extra = spec.extra or {}
    cfg = extra.get("llm_retry")
    if not isinstance(cfg, dict):
        cfg = {}
    max_retries = int(cfg.get("max_retries", DEFAULT_MAX_RETRIES))
    base_seconds = float(cfg.get("base_seconds", DEFAULT_BASE_SECONDS))
    max_seconds = float(cfg.get("max_seconds", DEFAULT_MAX_SECONDS))
    return max(0, max_retries), max(0.0, base_seconds), max(0.0, max_seconds)


def is_retryable_model_http_error(exc: BaseException) -> bool:
    return isinstance(exc, ModelHTTPError) and exc.status_code in RETRYABLE_STATUS_CODES


def retry_delay_seconds(
    attempt: int,
    *,
    base_seconds: float,
    max_seconds: float,
) -> float:
    delay = min(max_seconds, base_seconds * (2 ** attempt))
    return delay * (0.5 + random.random() * 0.5)
