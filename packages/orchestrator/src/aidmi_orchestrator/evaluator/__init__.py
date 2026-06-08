"""Evaluator modules register themselves at import time."""
from aidmi_orchestrator.evaluator import base  # noqa: F401
from aidmi_orchestrator.evaluator import execution  # noqa: F401
from aidmi_orchestrator.evaluator import llm_usage  # noqa: F401
from aidmi_orchestrator.evaluator import schema  # noqa: F401
from aidmi_orchestrator.evaluator import row_equality  # noqa: F401
from aidmi_orchestrator.evaluator import manifest_quality  # noqa: F401
from aidmi_orchestrator.evaluator import data_preservation  # noqa: F401
