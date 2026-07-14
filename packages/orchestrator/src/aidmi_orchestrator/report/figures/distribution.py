from __future__ import annotations

import re
import statistics
from pathlib import Path

from aidmi_orchestrator.report.aggregate import rep_values
from aidmi_orchestrator.report.theme import (
    apply_theme,
    color_for_cell,
    ordered_fixtures,
    strip_common_version,
)

# Same tokens as the other figures: text stays ink/muted, data color on marks.
_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"

# Fixtures have no fixed palette (unlike strategies), so colour them by position
# from a categorical ramp -- the colour only separates boxes, it carries no
# meaning of its own.
_FIXTURE_PALETTE = [
    "#4C78A8", "#F58518", "#54A24B", "#B279A2",
    "#E45756", "#72B7B2", "#EECA3B", "#9D755D",
]


# A run that materialized nothing (or silently produced an empty schema) has a
# null recall/field-acc/mat-rate. That is not missing data -- it is a
# zero-quality *outcome*, the left mode of the distribution, so nulls on the
# quality axes are zero-filled. Tokens and time are absolute-scale instrument
# readings; a null there is genuinely missing (not "0 tokens"), so it drops out.
def _outcome(getter):
    return lambda r: (v if (v := getter(r)) is not None else 0.0)


def _total_tokens(r):
    if r.tokens_in is None and r.tokens_out is None:
        return None
    return (r.tokens_in or 0) + (r.tokens_out or 0)


# Two columns of three panels each. Left column is the [0,1] quality axis
# (recall, field accuracy, materialization rate); right column is the
# absolute-scale cost/effort axis (dollars, combined tokens, wall-clock time).
_LEFT_METRICS = [
    ("Recall", _outcome(lambda r: r.recall)),
    ("Field accuracy", _outcome(lambda r: r.field_acc)),
    ("Mat. rate", _outcome(lambda r: r.tables_materialized)),
]
_RIGHT_METRICS = [
    ("Cost $", lambda r: r.cost),
    ("Tokens (in+out)", _total_tokens),
    ("Time (s)", lambda r: r.secs),
]

_JITTER_HALF_WIDTH = 0.28


def _jitter(n):
    if n <= 1:
        return [0.0]
    step = (2 * _JITTER_HALF_WIDTH) / (n - 1)
    return [-_JITTER_HALF_WIDTH + step * j for j in range(n)]


def _ranked_groups(records, key):
    """Groups ordered by median recall (nulls as 0) descending, then name."""
    recall = rep_values(records, key, _outcome(lambda r: r.recall))
    all_groups = sorted({key(r) for r in records})
    ranked = sorted(
        (g for g in all_groups if recall.get(g)),
        key=lambda g: (-statistics.median(recall[g]), g),
    )
    unranked = sorted(g for g in all_groups if not recall.get(g))
    return ranked + unranked


def _draw_panel(ax, groups, colors, values, label, *, unit_axis):
    data = [values.get(g, []) for g in groups]
    positions = list(range(len(groups)))
    non_empty = [(p, d) for p, d in zip(positions, data) if d]
    if non_empty:
        bp = ax.boxplot(
            [d for _, d in non_empty], positions=[p for p, _ in non_empty],
            widths=0.55, showfliers=False, patch_artist=True,
            medianprops=dict(color=_INK, linewidth=1.4),
            whiskerprops=dict(color=_MUTED), capprops=dict(color=_MUTED),
        )
        for (p, _), box in zip(non_empty, bp["boxes"]):
            box.set(facecolor=colors[p], alpha=0.25, edgecolor=_MUTED)
    for p, d in zip(positions, data):
        if not d:
            continue
        xs = [p + off for off in _jitter(len(d))]
        ax.scatter(
            xs, d, s=14, color=colors[p], alpha=0.7,
            edgecolors=_SURFACE, linewidths=0.3, zorder=3,
        )
    ax.set_xlim(-0.6, len(groups) - 0.4)
    if unit_axis:
        ax.set_ylim(-0.05, 1.08)
    else:
        ax.set_ylim(bottom=0)
    ax.set_ylabel(label, color=_INK)


def _dist_figure(records, out_dir, filename, salt, key, colors_for, title,
                 order_groups=None) -> Path:
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = salt
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    groups = order_groups(records) if order_groups else _ranked_groups(records, key)
    colors = colors_for(groups)
    nrows = max(len(_LEFT_METRICS), len(_RIGHT_METRICS))
    col_w = max(4.5, 0.9 * len(groups) + 2.0)
    fig, axes = plt.subplots(
        nrows=nrows, ncols=2, squeeze=False, sharex="col",
        figsize=(2 * col_w, 2.6 * nrows + 0.6),
    )
    columns = [(0, _LEFT_METRICS, True), (1, _RIGHT_METRICS, False)]
    for col, metrics, unit_axis in columns:
        for i, (label, getter) in enumerate(metrics):
            ax = axes[i][col]
            _draw_panel(ax, groups, colors, rep_values(records, key, getter),
                        label, unit_axis=unit_axis)
        bottom = axes[len(metrics) - 1][col]
        bottom.set_xticks(range(len(groups)))
        bottom.set_xticklabels(strip_common_version(groups), rotation=25,
                               ha="right", fontsize=9, color=_INK)

    fig.suptitle(title, color=_INK, fontsize=12, x=0.02, ha="left")
    fig.subplots_adjust(left=0.07, right=0.98, top=0.94, bottom=0.13,
                        wspace=0.16, hspace=0.15)

    out = out_dir / filename
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out


def _slug(value: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "_", str(value)).strip("_").lower()


def _strategy_colors(groups):
    return [color_for_cell(g) for g in groups]


def _fixture_colors(groups):
    return [_FIXTURE_PALETTE[i % len(_FIXTURE_PALETTE)] for i in range(len(groups))]


def _fixture_order(records):
    return ordered_fixtures({r.fixture for r in records})


def fig_dist_by_strategy(records, out_dir) -> Path:
    on = [r for r in records if r.sc is True]
    return _dist_figure(
        on, out_dir, "dist_by_strategy.svg", "aidmi-dist-strategy",
        lambda r: r.cell, _strategy_colors,
        "Per-strategy distribution (self-correction on) — box (IQR + median) over every run",
    )


def fig_dist_by_fixture(records, out_dir) -> Path:
    on = [r for r in records if r.sc is True]
    return _dist_figure(
        on, out_dir, "dist_by_fixture.svg", "aidmi-dist-fixture",
        lambda r: r.fixture, _fixture_colors,
        "Per-fixture distribution (self-correction on) — box (IQR + median) over every run",
        order_groups=_fixture_order,
    )


def _fk_values_by(records, key):
    """group -> list of non-null FK integrity values (nulls are 'no FK cells',
    not a zero outcome, so they drop out rather than zero-fill)."""
    groups: dict = {}
    for r in records:
        if r.fk_integrity is None:
            continue
        groups.setdefault(key(r), []).append(r.fk_integrity)
    return groups


def _fk_iqr_figure(records, out_dir, filename, salt, key, colors_for, title,
                   order_groups=None) -> Path:
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = salt
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    values = _fk_values_by(records, key)
    if order_groups:
        groups = [g for g in order_groups(records) if g in values]
    else:
        groups = sorted(values, key=lambda g: (-statistics.median(values[g]), g))
    colors = colors_for(groups)

    col_w = max(4.5, 0.9 * len(groups) + 2.0)
    fig, ax = plt.subplots(figsize=(col_w, 4.2))
    _draw_panel(ax, groups, colors, values, "FK integrity", unit_axis=True)
    ax.set_xticks(range(len(groups)))
    ax.set_xticklabels(strip_common_version(groups), rotation=25, ha="right",
                       fontsize=9, color=_INK)
    fig.suptitle(title, color=_INK, fontsize=12, x=0.02, ha="left")
    fig.subplots_adjust(left=0.1, right=0.98, top=0.92, bottom=0.2)

    out = out_dir / filename
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out


def fig_fk_iqr_by_strategy(records, out_dir) -> Path:
    on = [r for r in records if r.sc is True]
    return _fk_iqr_figure(
        on, out_dir, "fk_iqr_by_strategy.svg", "aidmi-fk-iqr-strategy",
        lambda r: r.cell, _strategy_colors,
        "FK integrity by strategy (self-correction on) — box (IQR + median) over every run",
    )


def fig_fk_iqr_by_fixture(records, out_dir) -> Path:
    on = [r for r in records if r.sc is True]
    return _fk_iqr_figure(
        on, out_dir, "fk_iqr_by_fixture.svg", "aidmi-fk-iqr-fixture",
        lambda r: r.fixture, _fixture_colors,
        "FK integrity by fixture (self-correction on) — box (IQR + median) over every run",
        order_groups=_fixture_order,
    )


def fig_dist_by_strategy_for_fixture(records, out_dir, fixture) -> Path:
    on = [r for r in records if r.sc is True and r.fixture == fixture]
    slug = _slug(fixture)
    return _dist_figure(
        on, out_dir, f"dist_strategy__{slug}.svg", f"aidmi-dist-strategy-{slug}",
        lambda r: r.cell, _strategy_colors,
        "Per-strategy distribution (self-correction on) — box (IQR + median) over every run",
    )


def fig_dist_by_fixture_for_strategy(records, out_dir, cell) -> Path:
    on = [r for r in records if r.sc is True and r.cell == cell]
    slug = _slug(cell)
    return _dist_figure(
        on, out_dir, f"dist_fixture__{slug}.svg", f"aidmi-dist-fixture-{slug}",
        lambda r: r.fixture, _fixture_colors,
        "Per-fixture distribution (self-correction on) — box (IQR + median) over every run",
        order_groups=_fixture_order,
    )
