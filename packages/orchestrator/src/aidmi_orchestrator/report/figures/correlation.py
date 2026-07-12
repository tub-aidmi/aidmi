from __future__ import annotations

import statistics
from pathlib import Path

from aidmi_orchestrator.report.theme import apply_theme, color_for_cell, marker_for_model

# Same tokens as the other figures: text stays ink/muted, data color on marks.
_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"
_FIT = "#2a78d6"

_DEFAULT_MARKER = "o"


def _sort_key(k):
    return tuple(str(part) for part in k)


def _total_tokens_k(r):
    """Total tokens in thousands -- the raw counts (hundreds of thousands) are
    too large to share an axis legibly, and dividing by 1000 leaves the
    correlation and fit unchanged."""
    if r.tokens_in is None and r.tokens_out is None:
        return None
    return ((r.tokens_in or 0) + (r.tokens_out or 0)) / 1000.0


def _correlation_scatter(
    records, out_dir, *, filename, salt, x_getter, y_getter, x_label, y_label,
    title, x_unit, y_unit, identity=False,
):
    """Per-run scatter of two metrics with a least-squares fit line and the
    Pearson correlation stated on the plot. Points are coloured by strategy
    (and shaped by model when several are present)."""
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = salt
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    points = [
        (x, y, r.cell, r.model)
        for r in records
        if (x := x_getter(r)) is not None and (y := y_getter(r)) is not None
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
        marker = marker_for_model(model) if multi_model else _DEFAULT_MARKER
        ax.scatter(
            [p[0] for p in pts], [p[1] for p in pts], marker=marker, s=55,
            color=color_for_cell(cell), alpha=0.55, edgecolors=_SURFACE,
            linewidths=0.6, zorder=3,
        )

    xs = [p[0] for p in points]
    ys = [p[1] for p in points]

    handles = [
        Line2D(
            [], [], marker=_DEFAULT_MARKER, linestyle="none", markersize=9,
            markerfacecolor=color_for_cell(c), markeredgecolor=_SURFACE, label=c,
        )
        for c in sorted({p[2] for p in points})
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

    if identity:
        ax.plot([0, 1], [0, 1], color=_MUTED, linestyle="--", lw=1.2, zorder=2,
                alpha=0.7)
        handles.append(
            Line2D([], [], color=_MUTED, lw=1.2, linestyle="--",
                   label=f"{x_label.lower()} = {y_label.lower()}")
        )

    have_fit = (
        len(points) >= 2
        and statistics.pstdev(xs) > 0
        and statistics.pstdev(ys) > 0
    )
    if have_fit:
        r = statistics.correlation(xs, ys)
        slope, intercept = statistics.linear_regression(xs, ys)
        x0, x1 = min(xs), max(xs)
        ax.plot([x0, x1], [slope * x0 + intercept, slope * x1 + intercept],
                color=_FIT, lw=2.0, zorder=4)
        handles.append(
            Line2D([], [], color=_FIT, lw=2.0,
                   label=f"least-squares fit (r = {r:+.2f}, n = {len(points)})")
        )

    if x_unit:
        ax.set_xlim(-0.03, 1.03)
    else:
        ax.set_xlim(left=0)
    if y_unit:
        ax.set_ylim(-0.03, 1.03)
    else:
        ax.set_ylim(bottom=0)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(title, color=_INK, fontsize=12, loc="left")

    if handles:
        leg = ax.legend(
            handles=handles, loc="upper left", bbox_to_anchor=(1.02, 1.0),
            labelcolor=_INK, alignment="left",
        )
        if leg.get_title():
            leg.get_title().set_color(_INK)

    fig.subplots_adjust(left=0.08, right=0.62, top=0.92, bottom=0.12)

    out = out_dir / filename
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out


def fig_recall_field_acc(records, out_dir) -> Path:
    """Recall vs field accuracy per evaluated run: do recovering more tables and
    getting their cells right move together?"""
    return _correlation_scatter(
        records, out_dir,
        filename="recall_field_acc.svg", salt="aidmi-recall-field-acc",
        x_getter=lambda r: r.recall, y_getter=lambda r: r.field_acc,
        x_label="Recall", y_label="Field accuracy",
        title="Recall vs field accuracy",
        x_unit=True, y_unit=True, identity=True,
    )


def fig_recall_mat_rate(records, out_dir) -> Path:
    """Recall vs materialization rate per run: does recovering more of the
    golden tables track building more of the target tables?"""
    return _correlation_scatter(
        records, out_dir,
        filename="recall_mat_rate.svg", salt="aidmi-recall-mat-rate",
        x_getter=lambda r: r.recall, y_getter=lambda r: r.tables_materialized,
        x_label="Recall", y_label="Mat. rate",
        title="Recall vs materialization rate",
        x_unit=True, y_unit=True, identity=True,
    )


def _vs_tokens(records, out_dir, *, filename, salt, x_getter, x_label, title):
    on = [r for r in records if r.sc is True]
    return _correlation_scatter(
        on, out_dir, filename=filename, salt=salt,
        x_getter=x_getter, y_getter=_total_tokens_k,
        x_label=x_label, y_label="Total tokens (in+out, thousands)",
        title=title, x_unit=True, y_unit=False,
    )


def fig_tokens_vs_recall(records, out_dir) -> Path:
    return _vs_tokens(
        records, out_dir, filename="corr_tokens_recall.svg",
        salt="aidmi-corr-tokens-recall", x_getter=lambda r: r.recall,
        x_label="Recall", title="Recall vs total tokens",
    )


def fig_tokens_vs_field_acc(records, out_dir) -> Path:
    return _vs_tokens(
        records, out_dir, filename="corr_tokens_field_acc.svg",
        salt="aidmi-corr-tokens-field-acc", x_getter=lambda r: r.field_acc,
        x_label="Field accuracy", title="Field accuracy vs total tokens",
    )


def fig_tokens_vs_mat_rate(records, out_dir) -> Path:
    return _vs_tokens(
        records, out_dir, filename="corr_tokens_mat_rate.svg",
        salt="aidmi-corr-tokens-mat-rate", x_getter=lambda r: r.tables_materialized,
        x_label="Mat. rate", title="Materialization rate vs total tokens",
    )
