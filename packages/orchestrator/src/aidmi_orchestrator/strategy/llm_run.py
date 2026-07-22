"""Shared kwargs for pydantic-ai Agent.run() (thinking budget, retries)."""

from __future__ import annotations

from aidmi_orchestrator.domain import ModelSpec


def google_run_kwargs(spec: ModelSpec) -> dict:
    """Return Agent.run() kwargs for Google Cloud models (thinking budget, retries)."""
    kwargs: dict = {"output_retries": 3}
    if spec.provider != "google_cloud":
        return kwargs
    extra = spec.extra or {}
    if "google_thinking_config" in extra:
        kwargs["model_settings"] = {
            "google_thinking_config": extra["google_thinking_config"]
        }
    else:
        kwargs["model_settings"] = {"google_thinking_config": {"thinking_budget": 2048}}
    return kwargs
