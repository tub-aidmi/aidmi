import json
from pathlib import Path
import pytest
from aidmi_orchestrator.pricing import PriceInfo, lookup_price, load_overrides


def test_lookup_openai_model_via_litellm():
    info = lookup_price(provider="openai", model_name="gpt-4o-mini")
    assert info is not None
    assert info.input_cost_per_token > 0
    assert info.output_cost_per_token > 0


def test_lookup_unknown_returns_none():
    info = lookup_price(provider="openai", model_name="totally-fake-nonexistent-zzz")
    assert info is None


def test_override_file_takes_precedence(tmp_path):
    override = tmp_path / "pricing.json"
    override.write_text(json.dumps({
        "openai/gpt-4o-mini": {
            "input_cost_per_token": 0.999,
            "output_cost_per_token": 0.998,
            "cached_input_cost_per_token": 0.111,
        },
    }))
    overrides = load_overrides(override)
    info = lookup_price(provider="openai", model_name="gpt-4o-mini", overrides=overrides)
    assert info is not None
    assert info.input_cost_per_token == 0.999
    assert info.cached_input_cost_per_token == 0.111


def test_custom_model_only_in_override(tmp_path):
    override = tmp_path / "pricing.json"
    override.write_text(json.dumps({
        "corporate/internal-llm-v1": {
            "input_cost_per_token": 0.0001,
            "output_cost_per_token": 0.0002,
        },
    }))
    overrides = load_overrides(override)
    info = lookup_price(provider="corporate", model_name="internal-llm-v1", overrides=overrides)
    assert info is not None
    assert info.input_cost_per_token == 0.0001
