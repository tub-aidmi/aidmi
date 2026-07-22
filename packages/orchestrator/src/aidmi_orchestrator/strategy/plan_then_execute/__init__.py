from aidmi_orchestrator.strategy.base import register_strategy
from aidmi_orchestrator.strategy.plan_then_execute.strategy import (
    PlanThenExecute,
    PlanThenExecuteConfig,
)

register_strategy("plan_then_execute", PlanThenExecute, PlanThenExecuteConfig)
