from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.aggregate import pass_rate, rep_values
from aidmi_orchestrator.report.theme import apply_theme, color_for_cell, marker_for_model

# Same tokens as pareto.py/levers.py: text stays ink/muted, data color on marks.
_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"
_MEAN_COLOR = "#52514e"  # same neutral token as pareto's frontier / levers' overall

_DEFAULT_MARKER = "o"
_CTX_SHORT = {"metadata_only": "meta", "live_query_tool": "live"}

# Horizontal spread of the per-rep dots around each config's x position. Fixed
# regardless of rep count so the figure stays legible whether a config has 3
# reps or (pooled across fixtures) a dozen.
_JITTER_HALF_WIDTH = 0.32


def _config_key(r):
    return (r.cell, r.ctx, r.sc, r.model)


def _sort_key(k):
    return (str(k[0]), str(k[1]), str(k[2]), str(k[3]))


def _tick_label(k):
    cell, ctx, sc, _model = k
    ctx_short = _CTX_SHORT.get(ctx, str(ctx))
    sc_short = "sc:on" if sc else "sc:off"
    return f"{cell}\n{ctx_short}, {sc_short}"


def _jitter_offsets(n):
    if n <= 1:
        return [0.0]
    step = (2 * _JITTER_HALF_WIDTH) / (n - 1)
    return [-_JITTER_HALF_WIDTH + step * j for j in range(n)]


def fig_rep_spread(records, out_dir) -> Path:
    """Per-config rep spread: exposes non-unanimous pass rates.

    Each config is one x position (sorted by pass rate ascending). The 0/3,
    1/3, 2/3, 3/3 story only shows up if the individual rep outcomes are
    drawn -- a bare mean pass-rate bar would flatten a 2/3 config to look like
    a smooth 0.67 and hide that it is actually non-unanimous.
    """
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = "aidmi-rep-spread"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rates = pass_rate(records, _config_key, lambda r: r.materialized)
    reps = rep_values(records, _config_key, lambda r: 1.0 if r.materialized else 0.0)
    keys = sorted(rates, key=lambda k: (rates[k], _sort_key(k)))

    multi_model = len({k[3] for k in keys}) > 1
    non_unanimous = sum(1 for k in keys if 0.0 < rates[k] < 1.0)

    # Legend width is content (longest cell name), not config count, so reserve
    # it in inches and convert to a fraction of the (variable) figure width --
    # a fixed *fraction* would either waste space or clip long labels.
    legend_in = 2.7
    width = max(9.0, 0.5 * len(keys) + 3.0 + legend_in)
    fig, ax = plt.subplots(figsize=(width, 5.5))

    for i, k in enumerate(keys):
        cell = k[0]
        model = k[3]
        color = color_for_cell(cell)
        marker = marker_for_model(model) if multi_model else _DEFAULT_MARKER
        values = reps.get(k, [])
        offsets = _jitter_offsets(len(values))
        xs = [i + off for off in offsets]
        ax.scatter(
            xs, values, marker=marker, s=40, color=color, alpha=0.55,
            edgecolors=_SURFACE, linewidths=0.5, zorder=3,
        )
        ax.scatter(
            [i], [rates[k]], marker="D", s=70, color=_MEAN_COLOR,
            edgecolors=_SURFACE, linewidths=0.8, zorder=4,
        )

    ax.set_xlim(-0.6, len(keys) - 0.4)
    ax.set_xticks(range(len(keys)))
    ax.set_xticklabels(
        [_tick_label(k) for k in keys], rotation=90, ha="center", fontsize=7,
        color=_INK,
    )
    ax.set_ylim(-0.15, 1.15)
    ax.set_yticks([0, 1])
    ax.set_yticklabels(["0 (fail)", "1 (materialized)"])
    ax.set_ylabel("Per-rep outcome")

    cells = sorted({k[0] for k in keys})
    handles = [
        Line2D(
            [], [], marker=_DEFAULT_MARKER, linestyle="none", markersize=8,
            markerfacecolor=color_for_cell(c), markeredgecolor=_SURFACE, label=c,
        )
        for c in cells
    ]
    handles.append(
        Line2D(
            [], [], marker="D", linestyle="none", markersize=8,
            markerfacecolor=_MEAN_COLOR, markeredgecolor=_SURFACE,
            label="mean pass-rate",
        )
    )
    if multi_model:
        for model in sorted({k[3] for k in keys}):
            handles.append(
                Line2D(
                    [], [], marker=marker_for_model(model), linestyle="none",
                    markersize=8, markerfacecolor=_MUTED,
                    markeredgecolor=_SURFACE, label=model,
                )
            )
    if handles:
        leg = ax.legend(
            handles=handles, loc="upper left", bbox_to_anchor=(1.01, 1.0),
            labelcolor=_INK, alignment="left", fontsize=9,
        )
        if leg.get_title():
            leg.get_title().set_color(_INK)

    if keys:
        pct = non_unanimous / len(keys)
        fig.text(
            0.99, 0.99,
            f"{non_unanimous}/{len(keys)} configs ({pct:.0%}) are "
            "non-unanimous across reps",
            fontsize=9, color=_MUTED, ha="right", va="top",
        )

    right = 1.0 - legend_in / width
    fig.subplots_adjust(left=0.04, right=right, top=0.9, bottom=0.32)

    out = out_dir / "rep_spread.svg"
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out
