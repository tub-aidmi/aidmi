import matplotlib

matplotlib.use("Agg")
from pathlib import Path

from aidmi_orchestrator.report.data import load_records
from aidmi_orchestrator.report.figures.efficiency import (
    fig_cost_drivers,
    fig_efficiency,
)
from aidmi_orchestrator.report.figures.tokens import fig_thinking_tokens

FIX = Path(__file__).parent / "fixtures" / "mini_results.jsonl"


def test_efficiency_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_efficiency(recs, tmp_path)
    assert out.name == "efficiency.svg"
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0


def test_cost_drivers_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_cost_drivers(recs, tmp_path)
    assert out.name == "cost_drivers.svg"
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0


def test_thinking_tokens_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_thinking_tokens(recs, tmp_path)
    assert out.name == "thinking_tokens.svg"
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0
