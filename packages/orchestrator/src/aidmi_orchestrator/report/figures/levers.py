from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.aggregate import group_mean, materialization_rate
from aidmi_orchestrator.report.theme import apply_theme, color_for_cell

# Same tokens as pareto.py: data color lives on the marks only, text stays ink/muted.
_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"
_OVERALL = "#52514e"  # same neutral token as pareto's frontier line

_DEFAULT_MARKER = "o"

# Model identity is carried by row facets here (one row per model), so marker
# shape is never varied by model within a panel -- that would be a redundant,
# spurious encoding of something the row title already says.
_SC_ORDER = [False, True]
_SC_LABELS = ["off", "on"]

_CTX_ORDER = ["metadata_only", "live_query_tool"]
_CTX_LABELS = ["metadata-only", "live-query"]


def _filter_valid(records, attr, order):
    return [r for r in records if getattr(r, attr) in order]


def _row_key(attr):
    return lambda r: (r.model, r.cell, getattr(r, attr))


def _total_tokens(r):
    if r.tokens_in is None and r.tokens_out is None:
        return None
    return (r.tokens_in or 0) + (r.tokens_out or 0)


def _draw_panel(ax, cells, model, mat_or_rec, overall, x_order, *, is_rate,
                unit_axis):
    for cell in cells:
        color = color_for_cell(cell)
        pts = [
            (i, mat_or_rec[(model, cell, x)])
            for i, x in enumerate(x_order)
            if (model, cell, x) in mat_or_rec
        ]
        if not pts:
            continue
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        if len(pts) >= 2:
            ax.plot(
                xs, ys, color=color, lw=2, marker=_DEFAULT_MARKER,
                markersize=7, markerfacecolor=color, markeredgecolor=_SURFACE,
                zorder=3,
            )
        else:
            ax.scatter(
                xs, ys, color=color, marker=_DEFAULT_MARKER, s=50,
                edgecolors=_SURFACE, linewidths=1.0, zorder=3,
            )

    overall_pts = [
        (i, overall[(model, x)]) for i, x in enumerate(x_order)
        if (model, x) in overall
    ]
    if len(overall_pts) >= 2:
        ox = [p[0] for p in overall_pts]
        oy = [p[1] for p in overall_pts]
        ax.plot(
            ox, oy, color=_OVERALL, lw=2.5, linestyle="--", marker="D",
            markersize=6, zorder=4, alpha=0.85,
        )

    ax.set_xlim(-0.35, len(x_order) - 1 + 0.35)
    ax.set_xticks(range(len(x_order)))
    if unit_axis:
        ax.set_ylim(-0.03, 1.03)
        if is_rate:
            from matplotlib.ticker import PercentFormatter

            ax.yaxis.set_major_formatter(PercentFormatter(xmax=1.0))
    else:
        ax.set_ylim(bottom=0)


_LEGEND_FONTSIZE = 8.5
# Rough width of one DejaVu Sans glyph at the legend font size, in inches
# (measured: a 30-char label spans ~2.1in at 8.5px). Used to size the legend
# gutter to the longest label instead of a hand-tuned constant.
_LEGEND_CHAR_IN = 0.07
_LEGEND_PAD_IN = 0.8  # marker + gap before the text + right breathing room


def _legend_width_in(cells) -> float:
    longest = max((len(str(c)) for c in cells), default=12)
    longest = max(longest, len("overall mean"))
    return _LEGEND_PAD_IN + longest * _LEGEND_CHAR_IN


def _legend(fig, cells, anchor_x):
    from matplotlib.lines import Line2D

    handles = [
        Line2D(
            [], [], marker=_DEFAULT_MARKER, linestyle="none", markersize=8,
            markerfacecolor=color_for_cell(c), markeredgecolor=_SURFACE,
            label=c,
        )
        for c in cells
    ]
    if not handles:
        return
    handles.append(
        Line2D(
            [], [], color=_OVERALL, lw=2.5, linestyle="--", marker="D",
            markersize=6, alpha=0.85, label="overall mean",
        )
    )
    leg = fig.legend(
        handles=handles, title="Strategy (cell)", loc="center left",
        bbox_to_anchor=(anchor_x, 0.5), labelcolor=_INK, alignment="left",
        frameon=False, fontsize=_LEGEND_FONTSIZE,
    )
    leg.get_title().set_color(_INK)


def _slope_figure(
    records, out_dir, filename, salt, attr, x_order, x_labels, title,
    cost_by_model=None,
):
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = salt
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    filtered = _filter_valid(records, attr, x_order)
    key = _row_key(attr)
    model_x_key = lambda r: (r.model, getattr(r, attr))  # noqa: E731

    # Three stacked metric rows sharing the lever x-axis: materialization and
    # recall on a [0,1] axis, tokens on an absolute axis.
    metrics = [
        ("Materialization rate", materialization_rate(filtered, key),
         materialization_rate(filtered, model_x_key), True, True),
        ("Recall", group_mean(filtered, key, lambda r: r.recall),
         group_mean(filtered, model_x_key, lambda r: r.recall), False, True),
        ("Mean tokens/run (in+out)", group_mean(filtered, key, _total_tokens),
         group_mean(filtered, model_x_key, _total_tokens), False, False),
    ]

    models = sorted({r.model for r in filtered})
    cells = sorted({r.cell for r in filtered})
    cost_by_model = cost_by_model or {}
    n_cols = max(len(models), 1)

    # Lay the figure out in inches: a fixed y-label gutter, generously sized
    # plot columns (so the panels stay large once the page scales the SVG to a
    # fixed width), and a legend gutter sized to the longest strategy label.
    left_in, plot_in, gap_in = 0.85, 5.4, 0.2
    legend_in = _legend_width_in(cells)
    fig_w = left_in + plot_in * n_cols + gap_in + legend_in
    fig_h = 9.2

    fig, axes = plt.subplots(
        nrows=len(metrics), ncols=n_cols, sharex=True, squeeze=False,
        figsize=(fig_w, fig_h),
    )

    for j, model in enumerate(models):
        for i, (ylabel, values, overall, is_rate, unit_axis) in enumerate(metrics):
            ax = axes[i][j]
            _draw_panel(ax, cells, model, values, overall, x_order,
                        is_rate=is_rate, unit_axis=unit_axis)
            ax.set_ylabel(ylabel)
        top = axes[0][j]
        # Model header (multi-model only); the ctx cost delta is scoped to THIS
        # model column so a pooled Δ never masks opposite per-model trends.
        header = model if len(models) > 1 else ""
        if model in cost_by_model:
            header = f"{header}  {cost_by_model[model]}".strip()
        if header:
            top.set_title(header, loc="left", color=_MUTED, fontsize=9.5)
        axes[-1][j].set_xticks(range(len(x_order)))
        axes[-1][j].set_xticklabels(x_labels)

    fig.suptitle(title, color=_INK, fontsize=13, x=0.05, y=0.99, ha="left")

    plot_right = left_in + plot_in * n_cols
    _legend(fig, cells, anchor_x=(plot_right + gap_in) / fig_w)
    fig.subplots_adjust(
        left=left_in / fig_w, right=plot_right / fig_w,
        top=0.9, bottom=0.07, hspace=0.18,
    )

    out = out_dir / filename
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out


def fig_lever_sc(records, out_dir) -> Path:
    return _slope_figure(
        records, out_dir, "lever_sc.svg", "aidmi-lever-sc",
        attr="sc", x_order=_SC_ORDER, x_labels=_SC_LABELS,
        title="Lever: self-correction (off -> on)",
    )


def fig_lever_ctx(records, out_dir) -> Path:
    filtered = _filter_valid(records, "ctx", _CTX_ORDER)
    cost = group_mean(filtered, lambda r: (r.model, r.ctx), lambda r: r.cost)
    cost_by_model = {}
    for model in sorted({m for m, _ in cost}):
        meta = cost.get((model, "metadata_only"))
        live = cost.get((model, "live_query_tool"))
        if meta is None or live is None:
            continue
        delta = live - meta
        sign = "+" if delta >= 0 else "-"
        cost_by_model[model] = (
            f"Mean cost/run: metadata-only ${meta:.3f}, "
            f"live-query ${live:.3f} (Δ {sign}${abs(delta):.3f})"
        )
    return _slope_figure(
        records, out_dir, "lever_ctx.svg", "aidmi-lever-ctx",
        attr="ctx", x_order=_CTX_ORDER, x_labels=_CTX_LABELS,
        title="Lever: context mode (metadata-only -> live-query)",
        cost_by_model=cost_by_model,
    )
