from aidmi_orchestrator.strategy.base import register_strategy
from aidmi_orchestrator.strategy.mock.strategy import MockStrategy, MockStrategyConfig

register_strategy("mock", MockStrategy, MockStrategyConfig)
