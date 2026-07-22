from __future__ import annotations

import statistics
from collections import defaultdict
from pathlib import Path

from aidmi_orchestrator.report.theme import (
    apply_theme,
    cells_covering_states,
    color_for_cell,
    ordered_cells,
)

_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"
_ERR = "#898781"

# Second state: dense white diagonal over the bar's own fill, no bar outline.
# First state is a flat solid fill.
_HATCH = "///"
_HATCH_LW = 0.5
_HATCH_COLOR = "#ffffff"


def _total_tokens(r):
    if r.tokens_in is None and r.tokens_out is None:
        return None
    return (r.tokens_in or 0) + (r.tokens_out or 0)


# ±std annotation formatters, one per value scale.
def _std_unit(v):
    return f"±{v:.2f}"


def _std_cost(v):
    return f"±${v:.2f}"


def _std_tokens(v):
    return f"±{v / 1000:.0f}k" if v >= 1000 else f"±{v:.0f}"


# (key, y label, per-run getter, zero-fill missing as 0, unit [0,1] axis, ±std
# formatter). Mirrors the lever figures: recall/field-accuracy/tokens/cost
# average only evaluable runs, materialization zero-fills a run that produced
# nothing.
_METRICS = [
    ("recall", "Recall", lambda r: r.recall, False, True, _std_unit),
    ("field_acc", "Field accuracy", lambda r: r.field_acc, False, True, _std_unit),
    ("fk_integrity", "FK integrity", lambda r: r.fk_integrity, False, True, _std_unit),
    ("mat_rate", "Mat. rate", lambda r: r.tables_materialized, True, True, _std_unit),
    ("cost", "Cost/run ($)", lambda r: r.cost, False, False, _std_cost),
    ("tokens", "Tokens/run (in+out)", _total_tokens, False, False, _std_tokens),
]

# (key, title fragment, attribute, state order, state labels).
_LEVERS = [
    ("sc", "self-correction", "sc", [False, True], ["off", "on"]),
    (
        "ctx",
        "context mode",
        "ctx",
        ["metadata_only", "live_query_tool"],
        ["metadata-only", "live-query"],
    ),
]


def _grouped_values(records, attr, state_order, getter, zero_fill):
    """(cell, state) -> list of per-run metric values."""
    acc = defaultdict(list)
    for r in records:
        if getattr(r, attr) not in state_order:
            continue
        v = getter(r)
        if v is None:
            if not zero_fill:
                continue
            v = 0.0
        acc[(r.cell, getattr(r, attr))].append(float(v))
    return acc


def _mean_bar(values):
    """(height, yerr_low, yerr_high, std) — mean; std is annotated as text (no
    whiskers), 0 for a single value."""
    mean = sum(values) / len(values)
    std = statistics.pstdev(values) if len(values) > 1 else 0.0
    return mean, 0.0, 0.0, std


def _median_bar(values):
    """(height, yerr_low, yerr_high, None) — median with IQR (25th-75th)
    whiskers. IQR pairs with the median: both are percentiles, always inside the
    data range. A single value gets no whisker."""
    med = statistics.median(values)
    if len(values) < 2:
        return med, 0.0, 0.0, None
    q1, _, q3 = statistics.quantiles(values, n=4)
    # quantiles() (exclusive) and median() are different estimators, so on tiny
    # samples q1/q3 can land on the far side of the median; clamp to keep the
    # whisker non-negative (a no-op once there are enough reps).
    return med, max(0.0, med - q1), max(0.0, q3 - med), None


def _runs_subtitle(stat_label, counts):
    """'<stat> per bar · n=<k>/bar · N=<total> runs', a range for n when bars
    differ — mirrors the heatmap subtitle. States mean vs median in-figure."""
    total = sum(counts)
    if not counts:
        return f"{stat_label} per bar"
    lo, hi = min(counts), max(counts)
    per = f"n={lo}/bar" if lo == hi else f"n={lo}–{hi}/bar"
    return f"{stat_label} per bar · {per} · N={total} runs"


def _bar_figure(
    records,
    out_dir,
    *,
    filename,
    salt,
    title,
    attr,
    state_order,
    state_labels,
    getter,
    y_label,
    zero_fill,
    unit_axis,
    summarise,
    stat_label,
    fmt_std,
):
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = salt
    mpl.rcParams["hatch.linewidth"] = _HATCH_LW
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    grouped = _grouped_values(records, attr, state_order, getter, zero_fill)
    # Only strategies run in every lever state belong on a lever comparison.
    covered = cells_covering_states(records, attr, state_order)
    cells = [c for c in ordered_cells({c for c, _ in grouped}) if c in covered]

    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    n_states = len(state_order)
    width = 0.8 / n_states
    max_h = 0.0
    annotations = []  # (x, height, std) for mean bars
    bar_counts = []  # per-bar run count, summarised into the subtitle
    for si, state in enumerate(state_order):
        offset = (si - (n_states - 1) / 2) * width
        xs, heights, lo, hi, colors = [], [], [], [], []
        for ci, cell in enumerate(cells):
            vals = grouped.get((cell, state))
            if not vals:
                continue
            h, el, eh, std = summarise(vals)
            xs.append(ci + offset)
            heights.append(h)
            lo.append(el)
            hi.append(eh)
            color = color_for_cell(cell)
            colors.append(color)
            max_h = max(max_h, h + eh)
            bar_counts.append(len(vals))
            if std is not None:
                annotations.append((ci + offset, h, std))
        if not xs:
            continue
        has_err = any(el or eh for el, eh in zip(lo, hi, strict=False))
        textured = si > 0
        ax.bar(
            xs,
            heights,
            width,
            color=colors,
            linewidth=0,
            edgecolor=(_HATCH_COLOR if textured else "none"),
            hatch=(_HATCH if textured else None),
            zorder=3,
            yerr=([lo, hi] if has_err else None),
            error_kw=dict(ecolor=_ERR, elinewidth=0.9, capsize=2, zorder=4),
        )

    # Std shown as a small label above each mean bar rather than as whiskers.
    for x, h, std in annotations:
        ax.annotate(
            fmt_std(std),
            (x, h),
            textcoords="offset points",
            xytext=(0, 2),
            ha="center",
            va="bottom",
            fontsize=7,
            color=_MUTED,
        )

    # Strip chrome: faint horizontal reference lines only, no vertical grid, no
    # left spine -- the bars carry the data, not a boxed lattice.
    ax.grid(False)
    ax.grid(True, axis="y", color=_MUTED, alpha=0.18, linewidth=0.6)
    ax.set_axisbelow(True)
    ax.tick_params(length=0)

    ax.set_xticks(range(len(cells)))
    ax.set_xticklabels(cells, rotation=25, ha="right", fontsize=9, color=_MUTED)
    ax.set_ylabel(y_label, color=_INK)
    # Headroom so the top annotation is not clipped by the axis.
    head = 1.15 if annotations else 1.05
    if unit_axis:
        ax.set_ylim(0, head)
    else:
        ax.set_ylim(0, max_h * head if max_h else None)
    ax.set_title(title, color=_INK, fontsize=12, loc="left", pad=20)
    ax.text(
        0.0,
        1.015,
        _runs_subtitle(stat_label, bar_counts),
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=9,
        color=_MUTED,
    )

    state_handles = [
        Patch(facecolor=_MUTED, edgecolor="none", label=state_labels[0]),
        Patch(
            facecolor=_MUTED,
            edgecolor=_HATCH_COLOR,
            hatch=_HATCH,
            linewidth=0,
            label=state_labels[1],
        ),
    ]
    leg = ax.legend(
        handles=state_handles,
        loc="best",
        frameon=False,
        fontsize=9,
        labelcolor=_INK,
    )
    if leg:
        leg.set_zorder(5)

    fig.tight_layout()
    out = out_dir / filename
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out


_SUMMARISERS = {"mean": _mean_bar, "median": _median_bar}


def build_bar_figures(records, out_dir) -> dict[str, list[Path]]:
    """One bar figure per (metric x lever) for each of mean and median. Returns
    {stat: [paths in lever-major, metric order]}."""
    out: dict[str, list[Path]] = {"mean": [], "median": []}
    for stat, summarise in _SUMMARISERS.items():
        for lever_key, frag, attr, state_order, state_labels in _LEVERS:
            for metric_key, y_label, getter, zero_fill, unit_axis, fmt_std in _METRICS:
                path = _bar_figure(
                    records,
                    out_dir,
                    filename=f"bar_{stat}_{lever_key}_{metric_key}.svg",
                    salt=f"aidmi-bar-{stat}-{lever_key}-{metric_key}",
                    title=f"{y_label} by strategy — {frag}",
                    attr=attr,
                    state_order=state_order,
                    state_labels=state_labels,
                    getter=getter,
                    y_label=y_label,
                    zero_fill=zero_fill,
                    unit_axis=unit_axis,
                    summarise=summarise,
                    stat_label=stat,
                    fmt_std=fmt_std,
                )
                out[stat].append(path)
    return out
