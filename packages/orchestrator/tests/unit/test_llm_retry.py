from pydantic_ai.exceptions import ModelHTTPError

from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.llm_retry import (
    RETRYABLE_STATUS_CODES,
    is_retryable_model_http_error,
    resolve_retry_settings,
    retry_delay_seconds,
)


def test_is_retryable_429():
    exc = ModelHTTPError(429, "gemini-2.5-flash", {"status": "RESOURCE_EXHAUSTED"})
    assert is_retryable_model_http_error(exc)


def test_is_not_retryable_400():
    exc = ModelHTTPError(400, "gemini-2.5-flash", {"message": "bad request"})
    assert not is_retryable_model_http_error(exc)


def test_is_not_retryable_non_http():
    assert not is_retryable_model_http_error(ValueError("nope"))


def test_retry_delay_bounded():
    delay = retry_delay_seconds(10, base_seconds=2.0, max_seconds=60.0)
    assert 0 < delay <= 60.0


def test_retry_delay_grows_with_attempt():
    delays = [
        retry_delay_seconds(i, base_seconds=1.0, max_seconds=1000.0)
        for i in range(4)
    ]
    assert delays[1] >= delays[0] * 0.5
    assert delays[2] >= delays[1] * 0.5


def test_resolve_retry_settings_defaults():
    max_retries, base, max_delay = resolve_retry_settings(
        ModelSpec(provider="google_cloud", model_name="gemini-2.5-flash"),
    )
    assert max_retries == 5
    assert base == 2.0
    assert max_delay == 60.0


def test_resolve_retry_settings_from_extra():
    spec = ModelSpec(
        provider="google_cloud",
        model_name="gemini-2.5-flash",
        extra={"llm_retry": {"max_retries": 2, "base_seconds": 1.5, "max_seconds": 10}},
    )
    assert resolve_retry_settings(spec) == (2, 1.5, 10.0)


def test_retryable_status_codes_include_503():
    assert 503 in RETRYABLE_STATUS_CODES
