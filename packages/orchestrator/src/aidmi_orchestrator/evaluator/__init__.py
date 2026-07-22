"""Evaluator modules register themselves at import time."""

from aidmi_orchestrator.evaluator import (
    base,  # noqa: F401
    data_preservation,  # noqa: F401
    execution,  # noqa: F401
    ground_truth_field_accuracy,  # noqa: F401
    ground_truth_fk_integrity,  # noqa: F401
    ground_truth_notes,  # noqa: F401
    ground_truth_recall,  # noqa: F401
    llm_usage,  # noqa: F401
    manifest_quality,  # noqa: F401
    row_equality,  # noqa: F401
    schema,  # noqa: F401
)
