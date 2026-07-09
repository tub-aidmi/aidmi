from __future__ import annotations

import statistics
from pathlib import Path

from aidmi_orchestrator.report.theme import apply_theme, color_for_cell, marker_for_model

# Same tokens as the other figures: text stays ink/muted, data color on marks.
_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"

_DEFAULT_MARKER = "o"


def _sort_key(k):
    return tuple(str(part) for part in k)


def fig_recall_field_acc(records, out_dir) -> Path:
    """Recall-vs-field-accuracy scatter, one point per evaluated run.

    Recall counts how many golden tables were recovered; field accuracy scores
    the cells within the rows that matched. They answer different questions, so
    a run can score high on one and low on the other -- this figure shows how
    tightly (or loosely) the two quality axes actually move together, with the
    Pearson r spelled out.
    """
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = "aidmi-recall-field-acc"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    points = [
        (r.recall, r.field_acc, r.cell, r.model)
        for r in records
        if r.recall is not None and r.field_acc is not None
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

    ax.plot([0, 1], [0, 1], color=_MUTED, linestyle="--", lw=1.2, zorder=2, alpha=0.7)

    ax.set_xlim(-0.03, 1.03)
    ax.set_ylim(-0.03, 1.03)
    ax.set_xlabel("Recall")
    ax.set_ylabel("Field accuracy")

    cells = sorted({p[2] for p in points})
    handles = [
        Line2D(
            [], [], marker=_DEFAULT_MARKER, linestyle="none", markersize=9,
            markerfacecolor=color_for_cell(c), markeredgecolor=_SURFACE, label=c,
        )
        for c in cells
    ]
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
        Line2D([], [], color=_MUTED, lw=1.2, linestyle="--", label="recall = field acc")
    )
    if handles:
        leg = ax.legend(
            handles=handles, loc="upper left", bbox_to_anchor=(1.02, 1.0),
            labelcolor=_INK, alignment="left",
        )
        if leg.get_title():
            leg.get_title().set_color(_INK)

    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    if len(points) >= 2 and statistics.pstdev(xs) > 0 and statistics.pstdev(ys) > 0:
        r = statistics.correlation(xs, ys)
        fig.text(
            0.07, 0.02,
            f"Pearson r = {r:+.2f} over {len(points)} evaluated runs — "
            "recovering more tables and getting their cells right are "
            f"{'related' if abs(r) >= 0.3 else 'largely independent'}",
            fontsize=9, color=_MUTED, ha="left", va="bottom",
        )

    fig.subplots_adjust(left=0.08, right=0.62, top=0.95, bottom=0.17)

    out = out_dir / "recall_field_acc.svg"
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out
