from __future__ import annotations

from collections import Counter
from pathlib import Path

from aidmi_orchestrator.report.aggregate import pass_rate, rep_values
from aidmi_orchestrator.report.theme import apply_theme, color_for_cell, marker_for_model

# Same tokens as pareto.py/levers.py: text stays ink/muted, data color on marks.
_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"
_MEAN_COLOR = "#52514e"  # same neutral token as pareto's frontier / levers' overall

_DEFAULT_MARKER = "o"

# Horizontal spread of the per-rep dots around each config's x position. A config
# is exactly 3 reps here, so this is the spread of 3 dots -- kept small since with
# ~96 configs the columns sit close together.
_JITTER_HALF_WIDTH = 0.34


def _config_key(r):
    # Fixture is IN the key on purpose: the honest unit of reliability is the same
    # exact config (cell x fixture x ctx x sc x model) run N times. Pooling across
    # fixtures would conflate fixture difficulty with true replicate noise.
    return (r.cell, r.fixture, r.ctx, r.sc, r.model)


def _sort_key(k):
    return tuple(str(part) for part in k)


def _jitter_offsets(n):
    if n <= 1:
        return [0.0]
    step = (2 * _JITTER_HALF_WIDTH) / (n - 1)
    return [-_JITTER_HALF_WIDTH + step * j for j in range(n)]


def _level_label(level, reps_per_config):
    """A pass-rate as its k/N fraction when N reps back every config, else a %.

    The denominator stays fixed at N (so 0/3 and 3/3, not a reduced 0/1 or 1/1)
    -- the whole point is reading these as "k of the 3 reps passed".
    """
    if reps_per_config is None:
        return f"{level:.0%}"
    return f"{round(level * reps_per_config)}/{reps_per_config}"


def fig_rep_spread(records, out_dir) -> Path:
    """Per-config rep spread: exposes non-unanimous pass rates.

    Each config is one x column (sorted by pass rate ascending), and its
    individual rep outcomes (0/1) are drawn as jittered dots plus a diamond at
    the mean. A bare mean bar would flatten a 2/3 config to a smooth 0.67 and
    hide that it is actually non-unanimous -- the whole point is which configs
    disagree with themselves across identical reruns.
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

    multi_model = len({k[-1] for k in keys}) > 1
    non_unanimous = sum(1 for k in keys if 0.0 < rates[k] < 1.0)

    # When every config has the same rep count, mean pass-rates land on the k/N
    # lattice and we can label bands as fractions (0/3, 1/3, ...). Otherwise fall
    # back to percentages.
    rep_counts = {len(v) for v in reps.values()}
    reps_per_config = next(iter(rep_counts)) if len(rep_counts) == 1 else None

    # Legend/annotation gutter is content-sized (longest cell name), so reserve it
    # in inches and convert to a fraction of the (config-count-driven) width.
    legend_in = 3.0
    width = max(10.0, 0.16 * len(keys) + legend_in)
    fig, ax = plt.subplots(figsize=(width, 5.5))

    for i, k in enumerate(keys):
        cell = k[0]
        model = k[-1]
        color = color_for_cell(cell)
        marker = marker_for_model(model) if multi_model else _DEFAULT_MARKER
        values = reps.get(k, [])
        offsets = _jitter_offsets(len(values))
        xs = [i + off for off in offsets]
        ax.scatter(
            xs, values, marker=marker, s=16, color=color, alpha=0.6,
            edgecolors=_SURFACE, linewidths=0.3, zorder=3,
        )
        ax.scatter(
            [i], [rates[k]], marker="D", s=26, color=_MEAN_COLOR,
            edgecolors=_SURFACE, linewidths=0.4, zorder=4,
        )

    # Guide lines at the partial pass-rate bands the mean diamonds cluster onto,
    # plus a consolidated count block. How many configs sit at each band
    # (18 at 0/3, 19 at 1/3, ...) is the honest summary the figure exists to show.
    # The block goes top-left, which is empty: the lowest-ranked configs there are
    # all-fail (0/N), so no dots reach up into that corner.
    level_counts = Counter(rates[k] for k in keys)
    for level in sorted(level_counts):
        if 0.0 < level < 1.0:
            ax.axhline(level, color=_MUTED, linestyle=":", lw=0.8, alpha=0.5, zorder=1)
    band_lines = "\n".join(
        f"{_level_label(level, reps_per_config)}:  {level_counts[level]} configs"
        for level in sorted(level_counts, reverse=True)
    )
    ax.text(
        0.012, 0.98, band_lines, transform=ax.transAxes, fontsize=8.5,
        color=_MUTED, ha="left", va="top", linespacing=1.5,
    )

    ax.set_xlim(-0.6, len(keys) - 0.4)
    ax.set_xticks([])
    ax.set_xlabel(
        f"{len(keys)} configs (cell x fixture x ctx x sc x model), "
        "each 3 reps, sorted by pass-rate →",
        fontsize=10,
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
        for model in sorted({k[-1] for k in keys}):
            handles.append(
                Line2D(
                    [], [], marker=marker_for_model(model), linestyle="none",
                    markersize=8, markerfacecolor=_MUTED,
                    markeredgecolor=_SURFACE, label=model,
                )
            )
    if handles:
        leg = ax.legend(
            handles=handles, loc="upper left", bbox_to_anchor=(1.005, 1.0),
            labelcolor=_INK, alignment="left", fontsize=9,
        )
        if leg.get_title():
            leg.get_title().set_color(_INK)

    if keys:
        pct = non_unanimous / len(keys)
        fig.text(
            0.99, 0.985,
            f"{non_unanimous}/{len(keys)} configs ({pct:.0%}) are "
            "non-unanimous across their 3 reps",
            fontsize=9.5, color=_MUTED, ha="right", va="top",
        )

    right = 1.0 - legend_in / width
    fig.subplots_adjust(left=0.06, right=right, top=0.9, bottom=0.14)

    out = out_dir / "rep_spread.svg"
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out
