from aidmi_orchestrator.strategy.base import register_strategy
from aidmi_orchestrator.strategy.structured_per_table.strategy import (
    StructuredPerTable,
    StructuredPerTableConfig,
)

register_strategy("structured_per_table", StructuredPerTable, StructuredPerTableConfig)
