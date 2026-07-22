from aidmi_orchestrator.strategy.base import register_strategy
from aidmi_orchestrator.strategy.write_tools_freeform.strategy import (
    WriteToolsFreeform,
    WriteToolsFreeformConfig,
)

register_strategy("write_tools_freeform", WriteToolsFreeform, WriteToolsFreeformConfig)
