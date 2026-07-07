from __future__ import annotations

import statistics
from pathlib import Path

from aidmi_orchestrator.report.aggregate import rep_values
from aidmi_orchestrator.report.theme import apply_theme, color_for_cell

# Same tokens as the other figures: text stays ink/muted, data color on marks.
_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"
_MEAN_COLOR = "#52514e"

# f1, recall and field_acc are all continuous [0,1] quality axes. The scorecard
# already shows their *means*; this figure exists to show the spread the mean
# hides -- a strategy averaging 0.6 could be a tight cluster or a 0/1 coin flip,
# and only the box + raw dots tell them apart.
#
# A run that materialized nothing (or silently produced an empty schema) has a
# null score. That is not missing data -- it is a zero-quality *outcome*, and
# it is the left mode of the distribution. Dropping it (as group_mean does) is
# exactly the averaging-away this figure exists to undo, so nulls are zero-filled.
def _outcome(getter):
    return lambda r: (v if (v := getter(r)) is not None else 0.0)


_METRICS = [
    ("f1", "F1", _outcome(lambda r: r.f1)),
    ("recall", "Recall", _outcome(lambda r: r.recall)),
    ("field_acc", "Field accuracy", _outcome(lambda r: r.field_acc)),
]

_JITTER_HALF_WIDTH = 0.28


def _cell_key(r):
    return r.cell


def _jitter(n):
    if n <= 1:
        return [0.0]
    step = (2 * _JITTER_HALF_WIDTH) / (n - 1)
    return [-_JITTER_HALF_WIDTH + step * j for j in range(n)]


def _ranked_cells(records):
    """Cells ordered by median f1 (nulls as 0) descending."""
    f1 = rep_values(records, _cell_key, _outcome(lambda r: r.f1))
    all_cells = sorted({r.cell for r in records})
    ranked = sorted(
        (c for c in all_cells if f1.get(c)),
        key=lambda c: (-statistics.median(f1[c]), c),
    )
    unranked = sorted(c for c in all_cells if not f1.get(c))
    return ranked + unranked


def _draw_panel(ax, cells, values, label):
    data = [values.get(c, []) for c in cells]
    positions = list(range(len(cells)))
    non_empty = [(p, d) for p, d in zip(positions, data) if d]
    if non_empty:
        bp = ax.boxplot(
            [d for _, d in non_empty], positions=[p for p, _ in non_empty],
            widths=0.55, showfliers=False, patch_artist=True,
            medianprops=dict(color=_INK, linewidth=1.4),
            whiskerprops=dict(color=_MUTED), capprops=dict(color=_MUTED),
        )
        for (p, _), box in zip(non_empty, bp["boxes"]):
            box.set(facecolor=color_for_cell(cells[p]), alpha=0.25, edgecolor=_MUTED)
    for p, d in zip(positions, data):
        if not d:
            continue
        xs = [p + off for off in _jitter(len(d))]
        ax.scatter(
            xs, d, s=14, color=color_for_cell(cells[p]), alpha=0.7,
            edgecolors=_SURFACE, linewidths=0.3, zorder=3,
        )
    ax.set_xlim(-0.6, len(cells) - 0.4)
    ax.set_ylim(-0.05, 1.08)
    ax.set_ylabel(label, color=_INK)


def fig_metric_distribution(records, out_dir) -> Path:
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = "aidmi-metric-dist"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cells = _ranked_cells(records)
    fig, axes = plt.subplots(
        nrows=len(_METRICS), ncols=1, squeeze=False, sharex=True,
        figsize=(max(9.0, 1.2 * len(cells) + 3.0), 3.0 * len(_METRICS) + 0.6),
    )
    for i, (_, label, getter) in enumerate(_METRICS):
        ax = axes[i][0]
        _draw_panel(ax, cells, rep_values(records, _cell_key, getter), label)

    bottom = axes[-1][0]
    bottom.set_xticks(range(len(cells)))
    bottom.set_xticklabels(cells, rotation=25, ha="right", fontsize=9, color=_INK)

    fig.suptitle(
        "Per-strategy score distribution — box (IQR + median) over every run",
        color=_INK, fontsize=12, x=0.02, ha="left",
    )
    fig.subplots_adjust(left=0.09, right=0.98, top=0.93, bottom=0.16, hspace=0.15)

    out = out_dir / "metric_distribution.svg"
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out


def fig_score_histogram(records, out_dir) -> Path:
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = "aidmi-score-hist"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Null score = materialized nothing = a zero outcome (the left mode). Count
    # it, don't drop it, or the figure would erase the very failures it exists
    # to show alongside the perfect runs.
    vals = [r.f1 if r.f1 is not None else 0.0 for r in records]
    fails = sum(1 for r in records if r.f1 is None)
    fig, ax = plt.subplots(figsize=(7.2, 4.4))

    if vals:
        ax.hist(
            vals, bins=[i / 10 for i in range(11)], color="#2a78d6",
            edgecolor=_SURFACE, linewidth=0.6, zorder=3,
        )
        mean = statistics.fmean(vals)
        median = statistics.median(vals)
        ax.axvline(mean, color=_MEAN_COLOR, linestyle="--", lw=1.4, zorder=4)
        ax.axvline(median, color="#E45756", linestyle="-", lw=1.4, zorder=4)
        top = ax.get_ylim()[1]
        ax.text(mean, top * 0.98, f" mean {mean:.2f}", color=_MEAN_COLOR,
                fontsize=9, ha="left", va="top")
        ax.text(median, top * 0.88, f" median {median:.2f}", color="#E45756",
                fontsize=9, ha="left", va="top")
        if fails:
            ax.text(0.02, top * 0.98, f"{fails} runs scored nothing (→0)",
                    color=_MUTED, fontsize=8.5, ha="left", va="top")

    ax.set_xlim(0, 1)
    ax.set_xlabel("F1 (null score = 0 outcome)", color=_INK)
    ax.set_ylabel("Runs", color=_INK)
    ax.set_title(
        "F1 distribution across all runs — mass piles at 0 and 1, not the mean",
        color=_INK, fontsize=12, loc="left",
    )
    fig.tight_layout()

    out = out_dir / "score_histogram.svg"
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out
