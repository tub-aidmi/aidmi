from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.theme import (
    apply_theme, color_for_cell, marker_for_model, ordered_cells,
)

# Same tokens as pareto.py/levers.py: text stays ink/muted, data color on marks.
_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"

_DEFAULT_MARKER = "o"
_SATURATION_THRESHOLD = 0.99


def _sort_key(k):
    return tuple(str(part) for part in k)


def fig_prec_recall(records, out_dir) -> Path:
    """Precision-vs-recall scatter, one point per run.

    Precision saturates near 1.0 for nearly every run while recall spreads
    across the full range -- this is the evidence that f1 (which blends both)
    hides more than it shows, and recall alone is the discriminating metric.
    """
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = "aidmi-prec-recall"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    points = [
        (r.precision, r.recall, r.cell, r.model)
        for r in records
        if r.precision is not None and r.recall is not None
    ]
    multi_model = len({p[3] for p in points}) > 1
    group_key = (lambda p: (p[2], p[3])) if multi_model else (lambda p: (p[2],))
    groups = {}
    for p in points:
        groups.setdefault(group_key(p), []).append(p)

    fig, ax = plt.subplots(figsize=(9.5, 5.5))

    for gk in sorted(groups, key=_sort_key):
        pts = groups[gk]
        cell = pts[0][2]
        model = pts[0][3]
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        marker = marker_for_model(model) if multi_model else _DEFAULT_MARKER
        ax.scatter(
            xs, ys, marker=marker, s=55, color=color_for_cell(cell),
            alpha=0.55, edgecolors=_SURFACE, linewidths=0.6, zorder=3,
        )

    ax.axvline(1.0, color=_MUTED, linestyle="--", lw=1.5, zorder=2, alpha=0.8)

    ax.set_xlim(-0.03, 1.08)
    ax.set_ylim(-0.03, 1.03)
    ax.set_xlabel("Precision")
    ax.set_ylabel("Recall")

    cells = ordered_cells({p[2] for p in points})
    cell_handles = [
        Line2D(
            [], [], marker=_DEFAULT_MARKER, linestyle="none", markersize=9,
            markerfacecolor=color_for_cell(c), markeredgecolor=_SURFACE, label=c,
        )
        for c in cells
    ]
    handles = list(cell_handles)
    if multi_model:
        for model in sorted({p[3] for p in points}):
            handles.append(
                Line2D(
                    [], [], marker=marker_for_model(model), linestyle="none",
                    markersize=9, markerfacecolor=_MUTED,
                    markeredgecolor=_SURFACE, label=model,
                )
            )
    handles.append(
        Line2D([], [], color=_MUTED, lw=1.5, linestyle="--", label="precision = 1.0")
    )
    if handles:
        leg = ax.legend(
            handles=handles, loc="upper left", bbox_to_anchor=(1.02, 1.0),
            labelcolor=_INK, alignment="left",
        )
        if leg.get_title():
            leg.get_title().set_color(_INK)

    if points:
        saturated = sum(1 for p in points if p[0] >= _SATURATION_THRESHOLD)
        pct = saturated / len(points)
        fig.text(
            0.07, 0.02,
            f"{pct:.0%} of runs score precision >= {_SATURATION_THRESHOLD:g} -- "
            "precision is saturated; recall is the discriminating metric",
            fontsize=9, color=_MUTED, ha="left", va="bottom",
        )

    fig.subplots_adjust(left=0.08, right=0.62, top=0.95, bottom=0.17)

    out = out_dir / "prec_recall.svg"
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out
