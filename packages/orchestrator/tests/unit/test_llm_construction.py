import pytest
from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.llm import (
    make_llm, register_provider, list_providers,
)


def test_builtin_providers_are_registered():
    names = list_providers()
    for name in ("openai", "openai_compatible", "ollama", "anthropic", "litellm"):
        assert name in names


def test_unknown_provider_raises():
    spec = ModelSpec(provider="not_a_real_provider", model_name="x")
    with pytest.raises(ValueError, match="unknown provider"):
        make_llm(spec)


def test_double_registration_raises():
    def factory(spec):
        return None  # type: ignore
    register_provider("custom_test_unique_xyz", factory)
    with pytest.raises(ValueError, match="already registered"):
        register_provider("custom_test_unique_xyz", factory)


def test_custom_provider_can_be_registered_and_used(monkeypatch):
    sentinel = object()

    def factory(spec):
        return sentinel

    register_provider("my_corporate_xyz", factory)
    spec = ModelSpec(provider="my_corporate_xyz", model_name="x")
    result = make_llm(spec)
    assert result is sentinel


@pytest.mark.parametrize(
    ("base_url", "expected"),
    [
        (None, "http://localhost:11434/v1"),
        ("http://localhost:11434", "http://localhost:11434/v1"),
        ("http://localhost:11434/", "http://localhost:11434/v1"),
        ("http://localhost:11434/v1", "http://localhost:11434/v1"),
        ("http://remote:11434/v1/", "http://remote:11434/v1"),
    ],
)
def test_ollama_base_url_appends_v1(base_url, expected):
    from aidmi_orchestrator.llm import _ollama_base_url

    spec = ModelSpec(provider="ollama", model_name="llama3", base_url=base_url)
    assert _ollama_base_url(spec) == expected
