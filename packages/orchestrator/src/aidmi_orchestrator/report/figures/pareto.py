from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.aggregate import group_mean
from aidmi_orchestrator.report.theme import (
    apply_theme,
    color_for_cell,
    marker_for_model,
    ordered_cells,
)

# Chrome/ink tokens (dataviz reference palette, light surface). Text and the
# frontier line wear ink tokens; only the markers carry the series (cell) color.
_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"
_FRONTIER = "#52514e"

_DEFAULT_MARKER = "o"


def _config_key(r):
    return (r.cell, r.ctx, r.sc, r.model)


def _sort_key(k):
    return (str(k[0]), str(k[1]), str(k[2]), str(k[3]))


def _pareto_frontier(points):
    """Upper-left frontier for (minimize cost, maximize recall).

    Sort by ascending cost and keep each point whose recall exceeds the best
    recall seen among all cheaper points (the running-max staircase).
    """
    ordered = sorted(points, key=lambda p: (p[0], -p[1]))
    frontier = []
    best = float("-inf")
    for cost, recall in ordered:
        if recall > best:
            frontier.append((cost, recall))
            best = recall
    return frontier


def fig_pareto(records, out_dir) -> Path:
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = "aidmi-pareto"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cost = group_mean(records, _config_key, lambda r: r.cost)
    recall = group_mean(records, _config_key, lambda r: r.recall)
    # A point needs both a mean cost and mean recall; log-x requires cost > 0.
    keys = sorted(
        (k for k in cost if k in recall and cost[k] > 0), key=_sort_key
    )
    # Configs that have a recall but no positive cost can't sit on a log axis;
    # they are dropped from the scatter. Count them so the loss is visible, not
    # silent (free local models report $0 cost and would otherwise vanish).
    plotted = set(keys)
    omitted = sum(1 for k in recall if k not in plotted)

    multi_model = len({k[3] for k in keys}) > 1

    fig, ax = plt.subplots(figsize=(11.0, 5.0))

    for k in keys:
        cell, _ctx, sc, model = k
        x, y = cost[k], recall[k]
        color = color_for_cell(cell)
        marker = marker_for_model(model) if multi_model else _DEFAULT_MARKER
        if sc:  # self-correction on -> filled, with a surface ring for overlap
            ax.scatter(
                x, y, marker=marker, s=80, color=color,
                edgecolors=_SURFACE, linewidths=1.0, zorder=3,
            )
        else:  # self-correction off -> hollow (open) marker
            ax.scatter(
                x, y, marker=marker, s=80, facecolors="none",
                edgecolors=color, linewidths=1.6, zorder=3,
            )

    frontier = _pareto_frontier([(cost[k], recall[k]) for k in keys])
    if len(frontier) >= 2:
        fx = [p[0] for p in frontier]
        fy = [p[1] for p in frontier]
        ax.step(
            fx, fy, where="post", color=_FRONTIER, lw=2, alpha=0.45,
            zorder=2, solid_capstyle="round",
        )

    ax.set_xscale("log")
    _format_cost_axis(ax)
    ax.set_xlabel("Cost per run ($, log)")
    ax.set_ylabel("Recall")
    ax.set_ylim(-0.03, 1.03)
    ax.margins(x=0.08)

    _add_legends(fig, ax, keys, multi_model, Line2D)

    fig.subplots_adjust(left=0.07, right=0.62, top=0.95, bottom=0.17)
    if omitted:
        plural = "s" if omitted != 1 else ""
        fig.text(
            0.07, 0.02,
            f"{omitted} config{plural} with $0 cost omitted (log axis)",
            fontsize=9, color=_MUTED, ha="left", va="bottom",
        )
    out = out_dir / "pareto.svg"
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out


def _format_cost_axis(ax):
    from matplotlib.ticker import FuncFormatter, LogLocator

    dollars = FuncFormatter(lambda v, _pos: f"${v:g}")
    ax.xaxis.set_major_locator(LogLocator(base=10, subs=(1.0,), numticks=12))
    ax.xaxis.set_minor_locator(
        LogLocator(base=10, subs=(0.2, 0.3, 0.5), numticks=12)
    )
    ax.xaxis.set_major_formatter(dollars)
    ax.xaxis.set_minor_formatter(dollars)
    ax.tick_params(axis="x", which="minor", labelsize=9)


def _add_legends(fig, ax, keys, multi_model, Line2D):
    # Cell (color) legend: identity channel, one entry per cell present.
    cells = ordered_cells({k[0] for k in keys})
    cell_handles = [
        Line2D(
            [], [], marker=_DEFAULT_MARKER, linestyle="none", markersize=9,
            markerfacecolor=color_for_cell(c), markeredgecolor=_SURFACE,
            label=c,
        )
        for c in cells
    ]
    if cell_handles:
        leg1 = ax.legend(
            handles=cell_handles, title="Strategy (cell)",
            loc="upper left", bbox_to_anchor=(1.02, 1.0),
            labelcolor=_INK, alignment="left",
        )
        leg1.get_title().set_color(_INK)
        ax.add_artist(leg1)

    # Encoding legend: fill = self-correction, plus shape = model if multi-model.
    enc = [
        Line2D(
            [], [], marker=_DEFAULT_MARKER, linestyle="none", markersize=9,
            markerfacecolor=_MUTED, markeredgecolor=_SURFACE,
            label="self-correction on",
        ),
        Line2D(
            [], [], marker=_DEFAULT_MARKER, linestyle="none", markersize=9,
            markerfacecolor="none", markeredgecolor=_MUTED,
            label="self-correction off",
        ),
        Line2D([], [], color=_FRONTIER, lw=2, alpha=0.6, label="Pareto frontier"),
    ]
    if multi_model:
        for model in sorted({k[3] for k in keys}):
            enc.append(
                Line2D(
                    [], [], marker=marker_for_model(model), linestyle="none",
                    markersize=9, markerfacecolor=_MUTED,
                    markeredgecolor=_SURFACE, label=model,
                )
            )
    leg2 = ax.legend(
        handles=enc, title="Encoding",
        loc="lower left", bbox_to_anchor=(1.02, 0.0),
        labelcolor=_INK, alignment="left",
    )
    leg2.get_title().set_color(_INK)
