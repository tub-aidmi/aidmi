"""Benchmark report: tidy data, figures, and static HTML gallery."""

from aidmi_orchestrator.report.data import RunRecord, load_records, write_tidy_csv
from aidmi_orchestrator.report.driver import build_report

__all__ = ["RunRecord", "load_records", "write_tidy_csv", "build_report"]
