from __future__ import annotations
import matplotlib as mpl

CELL_COLORS = {
    "write_tools_freeform": "#4C78A8",
    "write_tools_freeform_inlinedbt": "#72B7B2",
    "ensemble_vote": "#F58518",
    "plan_write_critique": "#E45756",
    "plan_then_execute": "#B279A2",
    "write_then_critique": "#EECA3B",
    "structured_per_table": "#54A24B",
}
MODEL_MARKERS = {"gemini25flash": "o", "qwen35b": "s", "mistral128b": "^"}
_FALLBACK_C = "#888888"; _FALLBACK_M = "D"

def color_for_cell(cell): return CELL_COLORS.get(cell, _FALLBACK_C)
def marker_for_model(model): return MODEL_MARKERS.get(model, _FALLBACK_M)

def apply_theme():
    mpl.rcParams.update({
        "figure.figsize": (7, 4.5), "figure.dpi": 100,
        "svg.fonttype": "none", "font.size": 11,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "grid.alpha": 0.3, "axes.axisbelow": True,
        "legend.frameon": False,
    })
