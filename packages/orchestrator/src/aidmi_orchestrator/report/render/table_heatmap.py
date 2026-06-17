"""Table × model heatmap rendering for per-table metrics."""
from __future__ import annotations

from pathlib import Path

import numpy as np

from aidmi_orchestrator.report.plot_specs import TableModelHeatmapPlotSpec


def render_table_heatmap_svg(spec: TableModelHeatmapPlotSpec, svg_path: Path) -> None:
    try:
        import matplotlib
    except ImportError as e:
        raise RuntimeError(
            "matplotlib is not installed — install the plots extra: "
            "uv sync --extra plots (or pip install 'aidmi-orchestrator[plots]')"
        ) from e
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    matrix = spec.values
    finite = matrix[np.isfinite(matrix)]
    if finite.size == 0:
        return

    cmap = plt.get_cmap("YlGn").copy()
    cmap.set_bad(color="#d9d9d9")

    fig, ax = plt.subplots(
        figsize=(max(6.0, len(spec.col_labels) * 1.4), max(3.0, len(spec.row_labels) * 0.5)),
        constrained_layout=True,
    )
    im = ax.imshow(matrix, aspect="auto", cmap=cmap, vmin=0.0, vmax=1.0)

    ax.set_xticks(range(len(spec.col_labels)))
    ax.set_xticklabels(spec.col_labels, rotation=35, ha="right")
    ax.set_xlabel("Model")
    ax.set_yticks(range(len(spec.row_labels)))
    ax.set_yticklabels(spec.row_labels)
    ax.set_ylabel("Table")
    ax.set_title(
        f"Per-table {spec.metric} — {spec.strategy} — {spec.fixture}"
    )

    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            value = matrix[i, j]
            if not np.isfinite(value):
                label = "n/a"
            else:
                label = f"{value:.2f}"
            ax.text(j, i, label, ha="center", va="center", fontsize=9)

    fig.colorbar(im, ax=ax, fraction=0.035, pad=0.04)
    svg_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(svg_path, format="svg", bbox_inches="tight", pad_inches=0.1)
    plt.close(fig)
