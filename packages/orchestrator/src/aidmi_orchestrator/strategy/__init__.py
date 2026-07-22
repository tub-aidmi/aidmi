"""Strategy sub-packages register themselves at import time."""

from aidmi_orchestrator.strategy import (
    base,  # noqa: F401
    ensemble_vote,  # noqa: F401
    mock,  # noqa: F401
    plan_then_execute,  # noqa: F401
    plan_write_critique,  # noqa: F401
    structured_per_table,  # noqa: F401
    write_then_critique,  # noqa: F401
    write_tools_freeform,  # noqa: F401
)
