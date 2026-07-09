from __future__ import annotations

import statistics
from pathlib import Path

from aidmi_orchestrator.report.aggregate import rep_values
from aidmi_orchestrator.report.theme import apply_theme, color_for_cell

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


# Top-to-bottom: recall, field accuracy, materialization rate on the [0,1]
# quality axis; combined tokens and wall-clock time on absolute axes.
_DIST_METRICS = [
    ("Recall", _outcome(lambda r: r.recall), True),
    ("Field acc", _outcome(lambda r: r.field_acc), True),
    ("Mat rate", _outcome(lambda r: r.tables_materialized), True),
    ("Tokens (in+out)", _total_tokens, False),
    ("Time (s)", lambda r: r.secs, False),
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


def _dist_figure(records, out_dir, filename, salt, key, colors_for, title) -> Path:
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = salt
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    groups = _ranked_groups(records, key)
    colors = colors_for(groups)
    fig, axes = plt.subplots(
        nrows=len(_DIST_METRICS), ncols=1, squeeze=False, sharex=True,
        figsize=(max(9.0, 1.2 * len(groups) + 3.0), 2.6 * len(_DIST_METRICS) + 0.6),
    )
    for i, (label, getter, unit_axis) in enumerate(_DIST_METRICS):
        ax = axes[i][0]
        _draw_panel(ax, groups, colors, rep_values(records, key, getter), label,
                    unit_axis=unit_axis)

    bottom = axes[-1][0]
    bottom.set_xticks(range(len(groups)))
    bottom.set_xticklabels(groups, rotation=25, ha="right", fontsize=9, color=_INK)

    fig.suptitle(title, color=_INK, fontsize=12, x=0.02, ha="left")
    fig.subplots_adjust(left=0.10, right=0.98, top=0.95, bottom=0.13, hspace=0.15)

    out = out_dir / filename
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out


def fig_dist_by_strategy(records, out_dir) -> Path:
    on = [r for r in records if r.sc is True]
    return _dist_figure(
        on, out_dir, "dist_by_strategy.svg", "aidmi-dist-strategy",
        lambda r: r.cell,
        lambda groups: [color_for_cell(g) for g in groups],
        "Per-strategy distribution (self-correction on) — box (IQR + median) over every run",
    )


def fig_dist_by_fixture(records, out_dir) -> Path:
    on = [r for r in records if r.sc is True]
    return _dist_figure(
        on, out_dir, "dist_by_fixture.svg", "aidmi-dist-fixture",
        lambda r: r.fixture,
        lambda groups: [_FIXTURE_PALETTE[i % len(_FIXTURE_PALETTE)]
                        for i in range(len(groups))],
        "Per-fixture distribution (self-correction on) — box (IQR + median) over every run",
    )
