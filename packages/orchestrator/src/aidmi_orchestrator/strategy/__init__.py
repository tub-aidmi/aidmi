"""Strategy sub-packages register themselves at import time."""
from aidmi_orchestrator.strategy import base  # noqa: F401
from aidmi_orchestrator.strategy import mock  # noqa: F401
from aidmi_orchestrator.strategy import structured_per_table  # noqa: F401
from aidmi_orchestrator.strategy import write_tools_freeform  # noqa: F401
from aidmi_orchestrator.strategy import write_then_critique  # noqa: F401
