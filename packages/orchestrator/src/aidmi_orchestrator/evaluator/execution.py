"""ExecutionEvaluator — dbt run signals."""

from __future__ import annotations

from typing import Any

from aidmi_orchestrator.evaluator.base import (
    RunArtifacts,
    register_evaluator,
)


class ExecutionEvaluator:
    name = "execution"

    def applies_to(self, artifacts: RunArtifacts) -> bool:
        return True

    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]:
        final = artifacts.final_transform_result
        if final is None:
            return {
                "dbt_success": False,
                "dbt_models_succeeded": 0,
                "dbt_models_failed": 0,
                "dbt_error_messages": [],
                "strategy_status": artifacts.strategy_result.self_reported_status,
            }
        succeeded = [m for m in final.models if m.status == "success"]
        failed = [m for m in final.models if m.status != "success"]
        return {
            "dbt_success": final.overall_status == "success",
            "dbt_models_succeeded": len(succeeded),
            "dbt_models_failed": len(failed),
            "dbt_error_messages": [
                m.error_message for m in failed if getattr(m, "error_message", None)
            ],
            "strategy_status": artifacts.strategy_result.self_reported_status,
        }


register_evaluator("execution", ExecutionEvaluator)
