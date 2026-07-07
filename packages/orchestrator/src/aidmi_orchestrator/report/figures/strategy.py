from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.aggregate import group_mean, materialization_rate
from aidmi_orchestrator.report.theme import apply_theme, color_for_cell

# Same tokens as pareto.py/levers.py: text stays ink/muted, data color on marks.
_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"

# Model identity is carried by row facets (one row per model), matching
# levers.py: a bar's color never re-encodes the model a row title already
# names, so multi-model campaigns never need a spurious hue for it.

# scorecard groups three metrics inside one cell -- that needs its own
# categorical identity (cell color already means something else: the x-axis
# category itself). Fixed slot order from the dataviz reference palette,
# validated CVD-safe as a set (validate_palette.js "<hexes>" --mode light).
_METRIC_ORDER = ["materialization", "recall", "field_acc"]
_METRIC_LABELS = {
    "materialization": "Materialization %",
    "recall": "Recall",
    "field_acc": "Field accuracy",
}
_METRIC_COLORS = {
    "materialization": "#2a78d6",
    "recall": "#1baf7a",
    "field_acc": "#eda100",
}


def _cell_key(r):
    return r.cell


def _ranked_cells(records):
    """All cells present, ranked by mean recall descending.

    Cells with no recall at all (every rep failed before ground truth could
    be scored) still need a slot -- they sink to the end, ordered by name so
    the layout stays deterministic.
    """
    all_cells = sorted({r.cell for r in records})
    recall = group_mean(records, _cell_key, lambda r: r.recall)
    ranked = sorted((c for c in all_cells if c in recall), key=lambda c: (-recall[c], c))
    unranked = sorted(c for c in all_cells if c not in recall)
    return ranked + unranked


def _bar_labels(ax, xs, ys, fmt):
    for x, y in zip(xs, ys):
        ax.text(
            x, y, fmt.format(y), ha="center", va="bottom", fontsize=8,
            color=_MUTED,
        )


def _draw_scorecard_panel(ax, cells, metrics):
    from matplotlib.ticker import PercentFormatter

    n_metrics = len(_METRIC_ORDER)
    width = 0.8 / n_metrics
    for m_i, metric in enumerate(_METRIC_ORDER):
        values = metrics[metric]
        offset = (m_i - (n_metrics - 1) / 2) * width
        xs = [i + offset for i, c in enumerate(cells) if c in values]
        ys = [values[c] for c in cells if c in values]
        if not xs:
            continue
        ax.bar(
            xs, ys, width=width * 0.9, color=_METRIC_COLORS[metric],
            zorder=3,
        )
        _bar_labels(ax, xs, ys, "{:.0%}")

    ax.set_xlim(-0.5, len(cells) - 0.5)
    ax.set_xticks(range(len(cells)))
    ax.set_xticklabels(cells, rotation=25, ha="right", fontsize=9, color=_INK)
    ax.set_ylim(0, 1.12)
    ax.yaxis.set_major_formatter(PercentFormatter(xmax=1.0))


def _scorecard_legend(fig):
    from matplotlib.patches import Patch

    handles = [
        Patch(facecolor=_METRIC_COLORS[m], label=_METRIC_LABELS[m])
        for m in _METRIC_ORDER
    ]
    leg = fig.legend(
        handles=handles, loc="upper center", bbox_to_anchor=(0.5, 1.0),
        ncol=len(handles), labelcolor=_INK, frameon=False,
    )
    return leg


def fig_scorecard(records, out_dir) -> Path:
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = "aidmi-scorecard"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cells = _ranked_cells(records)
    models = sorted({r.model for r in records})
    rows = models if len(models) > 1 else [None]

    fig, axes = plt.subplots(
        nrows=len(rows), ncols=1, squeeze=False,
        figsize=(max(9.0, 1.15 * len(cells) + 3.0), 4.1 * len(rows) + 0.5),
    )

    for i, model in enumerate(rows):
        ax = axes[i][0]
        subset = records if model is None else [r for r in records if r.model == model]
        metrics = {
            "materialization": materialization_rate(subset, _cell_key),
            "recall": group_mean(subset, _cell_key, lambda r: r.recall),
            "field_acc": group_mean(subset, _cell_key, lambda r: r.field_acc),
        }
        _draw_scorecard_panel(ax, cells, metrics)
        if model is not None:
            ax.set_title(model, loc="left", color=_INK, fontsize=12)

    _scorecard_legend(fig)
    fig.subplots_adjust(
        left=0.07, right=0.98, top=0.86, bottom=0.32 / len(rows) + 0.08,
        hspace=0.75,
    )

    out = out_dir / "scorecard.svg"
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out


def _draw_single_metric_panel(ax, cells, values, fmt):
    xs = [i for i, c in enumerate(cells) if c in values]
    ys = [values[c] for c in cells if c in values]
    colors = [color_for_cell(c) for c in cells if c in values]
    if xs:
        ax.bar(xs, ys, width=0.6, color=colors, zorder=3)
        _bar_labels(ax, xs, ys, fmt)

    ax.set_xlim(-0.5, len(cells) - 0.5)
    ax.set_xticks(range(len(cells)))
    ax.set_xticklabels(cells, rotation=25, ha="right", fontsize=9, color=_INK)
    ax.set_ylim(bottom=0)


def fig_cost_latency(records, out_dir) -> Path:
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = "aidmi-cost-latency"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Same cell order as the scorecard (recall desc) so the two figures read
    # side by side without re-deriving a mental ranking.
    cells = _ranked_cells(records)
    models = sorted({r.model for r in records})
    rows = models if len(models) > 1 else [None]

    fig, axes = plt.subplots(
        nrows=len(rows), ncols=2, squeeze=False,
        figsize=(max(11.0, 1.3 * len(cells) + 3.0), 3.9 * len(rows) + 0.5),
    )

    for i, model in enumerate(rows):
        ax_cost, ax_secs = axes[i][0], axes[i][1]
        subset = records if model is None else [r for r in records if r.model == model]
        cost = group_mean(subset, _cell_key, lambda r: r.cost)
        secs = group_mean(subset, _cell_key, lambda r: r.secs)

        _draw_single_metric_panel(ax_cost, cells, cost, "${:.3f}")
        ax_cost.set_ylabel("Mean cost per run ($)")
        _draw_single_metric_panel(ax_secs, cells, secs, "{:.0f}s")
        ax_secs.set_ylabel("Mean latency (s)")

        if model is not None:
            ax_cost.set_title(model, loc="left", color=_INK, fontsize=12)

    fig.subplots_adjust(
        left=0.07, right=0.98, top=0.9, bottom=0.34 / len(rows) + 0.08,
        wspace=0.28, hspace=0.75,
    )

    out = out_dir / "cost_latency.svg"
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out
