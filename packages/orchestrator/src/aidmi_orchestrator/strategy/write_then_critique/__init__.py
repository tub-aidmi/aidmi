from aidmi_orchestrator.strategy.base import register_strategy
from aidmi_orchestrator.strategy.write_then_critique.strategy import (
    WriteThenCritique,
    WriteThenCritiqueConfig,
)

register_strategy("write_then_critique", WriteThenCritique, WriteThenCritiqueConfig)
