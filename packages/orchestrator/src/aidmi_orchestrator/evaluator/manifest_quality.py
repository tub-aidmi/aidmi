"""ManifestQualityEvaluator — structural scoring of the mapping explanation artifact."""

from __future__ import annotations

from typing import Any

from aidmi_orchestrator.evaluator.base import RunArtifacts, register_evaluator


class ManifestQualityEvaluator:
    name = "manifest_quality"

    def applies_to(self, artifacts: RunArtifacts) -> bool:
        return True

    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]:
        manifest = artifacts.strategy_result.manifest
        target = (
            artifacts.target_schema_input or artifacts.strategy_result.target_schema
        )
        written = artifacts.strategy_result.target_tables_written

        if manifest is None:
            return {
                "manifest_present": False,
                "manifest_table_coverage": 0.0 if written else None,
                "manifest_column_coverage": 0.0 if target is not None else None,
                "manifest_explanation_rate": 0.0,
                "manifest_reasoning_rate": 0.0,
            }

        notes_by_table = {n.target_table: n for n in manifest.tables}

        table_coverage = (
            sum(1 for t in written if t in notes_by_table) / len(written)
            if written
            else None
        )

        column_coverage = None
        if target is not None:
            total = 0
            covered = 0
            for t in target.tables:
                noted = (
                    {c.target_column for c in notes_by_table[t.name].column_notes}
                    if t.name in notes_by_table
                    else set()
                )
                for c in t.columns:
                    total += 1
                    if c.name in noted:
                        covered += 1
            column_coverage = covered / total if total else None

        all_notes = [c for n in manifest.tables for c in n.column_notes]
        explanation_rate = (
            sum(1 for c in all_notes if c.explanation.strip()) / len(all_notes)
            if all_notes
            else 0.0
        )
        reasoning_rate = (
            sum(1 for n in manifest.tables if n.reasoning.strip())
            / len(manifest.tables)
            if manifest.tables
            else 0.0
        )

        return {
            "manifest_present": True,
            "manifest_table_coverage": table_coverage,
            "manifest_column_coverage": column_coverage,
            "manifest_explanation_rate": explanation_rate,
            "manifest_reasoning_rate": reasoning_rate,
        }


register_evaluator("manifest_quality", ManifestQualityEvaluator)
