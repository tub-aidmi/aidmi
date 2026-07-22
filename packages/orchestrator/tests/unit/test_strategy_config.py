from aidmi_orchestrator.strategy.structured_per_table.strategy import (
    StructuredPerTableConfig,
)
from aidmi_orchestrator.strategy.write_then_critique.strategy import (
    WriteThenCritiqueConfig,
)
from aidmi_orchestrator.domain import ModelSpec

WRITER = ModelSpec(provider="litellm", model_name="ise-ollama/qwen3.6:35b-a3b")


def test_defaults_preserve_old_behavior():
    cfg = StructuredPerTableConfig(writer_model=WRITER)
    assert cfg.fixer_model is None
    assert cfg.validation_gate == "none"


def test_fixer_model_and_gate_accepted():
    cfg = WriteThenCritiqueConfig(
        writer_model=WRITER,
        fixer_model=ModelSpec(
            provider="litellm", model_name="academic/qwen3.5-397b-a17b"
        ),
        validation_gate="static",
    )
    assert cfg.fixer_model.model_name == "academic/qwen3.5-397b-a17b"
    assert cfg.validation_gate == "static"
