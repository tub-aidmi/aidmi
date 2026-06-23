from aidmi_orchestrator.strategy.base import register_strategy
from aidmi_orchestrator.strategy.plan_write_critique.strategy import (
    PlanWriteCritique,
    PlanWriteCritiqueConfig,
)

register_strategy("plan_write_critique", PlanWriteCritique, PlanWriteCritiqueConfig)
