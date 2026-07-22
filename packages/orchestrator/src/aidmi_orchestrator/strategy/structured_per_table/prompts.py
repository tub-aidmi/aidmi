"""Prompt templates for the structured_per_table strategy (now shared via structured_common)."""

from aidmi_orchestrator.strategy.structured_common import (
    WRITER_SYSTEM_PROMPT as SYSTEM_PROMPT,
)
from aidmi_orchestrator.strategy.structured_common import (
    per_table_user_prompt,
)

__all__ = ["SYSTEM_PROMPT", "per_table_user_prompt"]
