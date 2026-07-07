from __future__ import annotations

from pathlib import Path

import numpy as np

from aidmi_orchestrator.report.aggregate import group_mean, materialization_rate
from aidmi_orchestrator.report.theme import apply_theme, sequential_cmap

# Same tokens as pareto.py/levers.py: text stays ink/muted, color carries data.
_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"

_METRIC_LABELS = {
    "materialized": "Materialization rate",
    "field_acc": "Field accuracy",
}


def _cell_fixture_key(r):
    return (r.cell, r.fixture)


def _matrix_for_metric(records, metric, cells, fixtures):
    if metric == "materialized":
        values = materialization_rate(records, _cell_fixture_key)
    elif metric == "field_acc":
        values = group_mean(records, _cell_fixture_key, lambda r: r.field_acc)
    else:
        raise ValueError(f"unknown metric: {metric!r}")

    matrix = np.full((len(cells), len(fixtures)), np.nan)
    for i, cell in enumerate(cells):
        for j, fixture in enumerate(fixtures):
            v = values.get((cell, fixture))
            if v is not None:
                matrix[i, j] = v
    return matrix


def fig_heatmap(records, out_dir, *, metric: str, filename: str, title: str) -> Path:
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = f"aidmi-heatmap-{metric}"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cells = sorted({r.cell for r in records})
    fixtures = sorted({r.fixture for r in records})
    matrix = _matrix_for_metric(records, metric, cells, fixtures)

    cmap = sequential_cmap().copy()
    cmap.set_bad(color=_SURFACE)

    fig, ax = plt.subplots(
        figsize=(max(6.0, len(fixtures) * 1.6 + 1.8), max(3.2, len(cells) * 0.6 + 1.2))
    )
    im = ax.imshow(matrix, aspect="auto", cmap=cmap, vmin=0.0, vmax=1.0)

    ax.set_xticks(range(len(fixtures)))
    ax.set_xticklabels(fixtures, rotation=30, ha="right", color=_INK)
    ax.set_yticks(range(len(cells)))
    ax.set_yticklabels(cells, color=_INK)
    ax.tick_params(length=0)
    ax.grid(False)  # cell color is the encoding; gridlines would cut annotations
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_xlabel("Fixture", color=_INK)
    ax.set_ylabel("Cell", color=_INK)

    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            v = matrix[i, j]
            if not np.isfinite(v):
                continue
            text_color = "white" if v >= 0.5 else _INK
            ax.text(j, i, f"{v * 100:.0f}%", ha="center", va="center",
                     color=text_color, fontsize=10)

    cbar = fig.colorbar(im, ax=ax, fraction=0.035, pad=0.04)
    cbar.ax.yaxis.set_major_formatter(PercentFormatter(xmax=1.0))
    cbar.set_label(_METRIC_LABELS.get(metric, metric), color=_INK)
    cbar.ax.tick_params(colors=_MUTED)
    cbar.outline.set_visible(False)

    ax.set_title(title, color=_INK, fontsize=12, loc="left")
    fig.tight_layout()

    out = out_dir / filename
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out
