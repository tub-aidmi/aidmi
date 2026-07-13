from __future__ import annotations

import statistics
from pathlib import Path

import numpy as np

from aidmi_orchestrator.report.aggregate import (
    group_mean,
    materialization_rate,
    rep_values,
)
from aidmi_orchestrator.report.theme import (
    apply_theme,
    ordered_cells,
    ordered_fixtures,
    sequential_cmap,
    strip_common_version,
)

# Same tokens as pareto.py/levers.py: text stays ink/muted, color carries data.
_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"

_METRIC_LABELS = {
    "materialized": "Mat. rate",
    "field_acc": "Field accuracy",
}

# The colorful grid is held to a fixed square box via box_aspect, independent
# of row/column count; the surrounding canvas absorbs labels and the colorbar.
# _PLOT_W_IN is that grid's side length in inches.
_PLOT_W_IN = 4.6
_BOX_ASPECT = 1.0  # height / width -> square

# Font sizes scaled to the cell size above, so text stays legible relative to
# the fields rather than shrinking as the grid grows.
_FS_VALUE = 13
_FS_STD = 10
_FS_TICK = 12
_FS_AXLABEL = 12.5
_FS_TITLE = 14
_FS_SUB = 10.5
_FS_CBAR = 11.5


def _cell_fixture_key(r):
    return (r.cell, r.fixture)


def _matrix_for_metric(records, metric, cells, fixtures):
    if metric == "materialized":
        values = materialization_rate(records, _cell_fixture_key)
    elif metric == "field_acc":
        values = group_mean(records, _cell_fixture_key, lambda r: r.field_acc)
    else:
        raise ValueError(f"unknown metric: {metric!r}")

    matrix = np.full((len(cells), len(fixtures)), np.nan)
    for i, cell in enumerate(cells):
        for j, fixture in enumerate(fixtures):
            v = values.get((cell, fixture))
            if v is not None:
                matrix[i, j] = v
    return matrix


def fig_heatmap(records, out_dir, *, metric: str, filename: str, title: str) -> Path:
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = f"aidmi-heatmap-{metric}"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cells = ordered_cells({r.cell for r in records})
    fixtures = ordered_fixtures({r.fixture for r in records})
    matrix = _matrix_for_metric(records, metric, cells, fixtures)

    cmap = sequential_cmap().copy()
    cmap.set_bad(color=_SURFACE)

    fig, ax = plt.subplots(
        figsize=(_PLOT_W_IN + 3.8, _PLOT_W_IN * _BOX_ASPECT + 2.6)
    )
    im = ax.imshow(matrix, aspect="auto", cmap=cmap, vmin=0.0, vmax=1.0)
    ax.set_box_aspect(_BOX_ASPECT)

    ax.set_xticks(range(len(fixtures)))
    ax.set_xticklabels(strip_common_version(fixtures), rotation=30, ha="right", color=_INK)
    ax.set_yticks(range(len(cells)))
    ax.set_yticklabels(cells, color=_INK)
    ax.tick_params(length=0)
    ax.grid(False)  # cell color is the encoding; gridlines would cut annotations
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_xlabel("Fixture", color=_INK)
    ax.set_ylabel("Cell", color=_INK)

    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            v = matrix[i, j]
            if not np.isfinite(v):
                continue
            text_color = "white" if v >= 0.5 else _INK
            ax.text(j, i, f"{v * 100:.0f}%", ha="center", va="center",
                     color=text_color, fontsize=10)

    cbar = fig.colorbar(im, ax=ax, fraction=0.035, pad=0.04)
    cbar.ax.yaxis.set_major_formatter(PercentFormatter(xmax=1.0))
    cbar.set_label(_METRIC_LABELS.get(metric, metric), color=_INK)
    cbar.ax.tick_params(colors=_MUTED)
    cbar.outline.set_visible(False)

    ax.set_title(title, color=_INK, fontsize=12, loc="left")
    fig.tight_layout()

    out = out_dir / filename
    fig.savefig(out, format="svg", metadata={"Date": None},
                bbox_inches="tight", pad_inches=0.12)
    plt.close(fig)
    return out


def _tokens(r):
    if r.tokens_in is None and r.tokens_out is None:
        return None
    return (r.tokens_in or 0) + (r.tokens_out or 0)


# key -> (title, colorbar label, value getter, zero_fill, unit_scale, cell_fmt).
# Unit-scale metrics share a fixed [0,1] percent axis; absolute metrics get an
# auto-scaled axis and their own cell formatting. Recall and materialization
# rate zero-fill (a run that produced nothing scored 0); the rest average only
# the runs that produced something.
_HEATMAP_METRICS = {
    "recall": ("Recall", "Recall", lambda r: r.recall, True, True,
               lambda v: f"{v * 100:.0f}%"),
    "field_acc": ("Field accuracy", "Field accuracy", lambda r: r.field_acc,
                  False, True, lambda v: f"{v * 100:.0f}%"),
    "mat_rate": ("Materialization rate", "Mat. rate",
                 lambda r: r.tables_materialized, True, True,
                 lambda v: f"{v * 100:.0f}%"),
    "cost": ("Cost", "Mean cost/run ($)", lambda r: r.cost, False, False,
             lambda v: f"${v:.2f}"),
    "tokens": ("Tokens (in+out)", "Mean tokens/run (in+out)", _tokens, False,
               False, lambda v: f"{v / 1000:.0f}k" if v >= 1000 else f"{v:.0f}"),
    "time": ("Time", "Mean time/run (s)", lambda r: r.secs, False, False,
             lambda v: f"{v:.0f}s"),
    "retries": ("LLM retries", "Mean LLM retries/run", lambda r: r.retries,
                True, False, lambda v: f"{v:.1f}"),
}


def _cell_stats(records, getter, zero_fill):
    """(cell, fixture) -> (mean, pop-std, n) over the runs feeding each cell.
    Zero-fill metrics count a None run as a 0; others average only the runs that
    produced a value (so n is the evaluable count that formed the mean)."""
    from collections import defaultdict

    groups = defaultdict(list)
    for r in records:
        v = getter(r)
        if v is None:
            if not zero_fill:
                continue
            v = 0.0
        groups[(r.cell, r.fixture)].append(float(v))
    out = {}
    for key, vals in groups.items():
        mean = sum(vals) / len(vals)
        std = statistics.pstdev(vals) if len(vals) > 1 else 0.0
        out[key] = (mean, std, len(vals))
    return out


def _runs_subtitle(counts):
    """'mean per cell · n=<k>/cell · N=<total> runs', with a range for n when
    cells differ."""
    total = sum(counts)
    if not counts:
        return "mean per cell"
    lo, hi = min(counts), max(counts)
    per = f"n={lo}/cell" if lo == hi else f"n={lo}–{hi}/cell"
    return f"mean per cell · {per} · N={total} runs"


def fig_metric_heatmap(records, out_dir, *, key: str) -> Path:
    """Strategy (rows) x fixture (cols) heatmap for one metric, mean per cell."""
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    title_stub, cbar_label, getter, zero_fill, unit_scale, cell_fmt = \
        _HEATMAP_METRICS[key]

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = f"aidmi-heatmap-sc-on-{key}"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cells = ordered_cells({r.cell for r in records})
    fixtures = ordered_fixtures({r.fixture for r in records})
    stats = _cell_stats(records, getter, zero_fill)

    matrix = np.full((len(cells), len(fixtures)), np.nan)
    std_matrix = np.full((len(cells), len(fixtures)), np.nan)
    for i, cell in enumerate(cells):
        for j, fixture in enumerate(fixtures):
            cell_stat = stats.get((cell, fixture))
            if cell_stat is not None:
                matrix[i, j] = cell_stat[0]
                std_matrix[i, j] = cell_stat[1]

    finite = matrix[np.isfinite(matrix)]
    if unit_scale:
        vmax = 1.0
    else:
        vmax = max(1e-9, float(finite.max())) if finite.size else 1.0
    threshold = 0.5 * vmax

    cmap = sequential_cmap().copy()
    cmap.set_bad(color=_SURFACE)

    fig, ax = plt.subplots(
        figsize=(_PLOT_W_IN + 3.8, _PLOT_W_IN * _BOX_ASPECT + 2.6)
    )
    im = ax.imshow(matrix, aspect="auto", cmap=cmap, vmin=0.0, vmax=vmax)
    ax.set_box_aspect(_BOX_ASPECT)

    ax.set_xticks(range(len(fixtures)))
    ax.set_xticklabels(strip_common_version(fixtures), rotation=30, ha="right",
                       color=_INK, fontsize=_FS_TICK)
    ax.set_yticks(range(len(cells)))
    ax.set_yticklabels(cells, color=_INK, fontsize=_FS_TICK)
    ax.tick_params(length=0)
    ax.grid(False)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_xlabel("Fixture", color=_INK, fontsize=_FS_AXLABEL)
    ax.set_ylabel("Strategy", color=_INK, fontsize=_FS_AXLABEL)

    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            v = matrix[i, j]
            if not np.isfinite(v):
                continue
            text_color = "white" if v >= threshold else _INK
            std = std_matrix[i, j]
            # Mean centred; a second ±std line only when there is spread.
            if np.isfinite(std) and std > 0:
                ax.text(j, i - 0.13, cell_fmt(v), ha="center", va="center",
                        color=text_color, fontsize=_FS_VALUE)
                ax.text(j, i + 0.17, f"±{cell_fmt(std)}", ha="center",
                        va="center", color=text_color, fontsize=_FS_STD)
            else:
                ax.text(j, i, cell_fmt(v), ha="center", va="center",
                        color=text_color, fontsize=_FS_VALUE)

    cbar = fig.colorbar(im, ax=ax, fraction=0.035, pad=0.04)
    if unit_scale:
        cbar.ax.yaxis.set_major_formatter(PercentFormatter(xmax=1.0))
    cbar.set_label(cbar_label, color=_INK, fontsize=_FS_CBAR)
    cbar.ax.tick_params(colors=_MUTED, labelsize=_FS_TICK - 2)
    cbar.outline.set_visible(False)

    ax.set_title(
        f"{title_stub} — strategy × fixture (self-correction on)",
        color=_INK, fontsize=_FS_TITLE, loc="left", pad=22,
    )
    counts = [n for _, _, n in stats.values()]
    ax.text(0.0, 1.015, _runs_subtitle(counts), transform=ax.transAxes,
            ha="left", va="bottom", fontsize=_FS_SUB, color=_MUTED)
    fig.tight_layout()

    out = out_dir / f"heatmap_sc_on_{key}.svg"
    fig.savefig(out, format="svg", metadata={"Date": None},
                bbox_inches="tight", pad_inches=0.12)
    plt.close(fig)
    return out


def _f1_std_matrix(records, cells, fixtures):
    reps = rep_values(records, _cell_fixture_key, lambda r: r.f1)
    matrix = np.full((len(cells), len(fixtures)), np.nan)
    for i, cell in enumerate(cells):
        for j, fixture in enumerate(fixtures):
            vals = reps.get((cell, fixture))
            if vals and len(vals) >= 2:
                matrix[i, j] = statistics.pstdev(vals)
    return matrix


def fig_std_heatmap(records, out_dir, *, filename="heatmap_f1_std.svg",
                    title="F1 replicate std by strategy × fixture") -> Path:
    """Companion to the mean heatmaps: the *spread* behind each averaged cell.

    A dark cell is a strategy×fixture whose reported mean f1 hides wide
    rep-to-rep disagreement -- a number you should not trust as a point
    estimate. Red ramp so it never reads as a quality (blue) heatmap.
    """
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = "aidmi-heatmap-std"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cells = ordered_cells({r.cell for r in records})
    fixtures = ordered_fixtures({r.fixture for r in records})
    matrix = _f1_std_matrix(records, cells, fixtures)

    finite = matrix[np.isfinite(matrix)]
    vmax = max(0.1, float(finite.max())) if finite.size else 0.5

    cmap = sequential_cmap().copy()
    cmap.set_bad(color=_SURFACE)

    fig, ax = plt.subplots(
        figsize=(_PLOT_W_IN + 3.8, _PLOT_W_IN * _BOX_ASPECT + 2.6)
    )
    im = ax.imshow(matrix, aspect="auto", cmap=cmap, vmin=0.0, vmax=vmax)
    ax.set_box_aspect(_BOX_ASPECT)

    ax.set_xticks(range(len(fixtures)))
    ax.set_xticklabels(strip_common_version(fixtures), rotation=30, ha="right", color=_INK)
    ax.set_yticks(range(len(cells)))
    ax.set_yticklabels(cells, color=_INK)
    ax.tick_params(length=0)
    ax.grid(False)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_xlabel("Fixture", color=_INK)
    ax.set_ylabel("Cell", color=_INK)

    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            v = matrix[i, j]
            if not np.isfinite(v):
                continue
            text_color = "white" if v >= vmax * 0.5 else _INK
            ax.text(j, i, f"{v:.2f}", ha="center", va="center",
                     color=text_color, fontsize=10)

    cbar = fig.colorbar(im, ax=ax, fraction=0.035, pad=0.04)
    cbar.set_label("F1 std across reps", color=_INK)
    cbar.ax.tick_params(colors=_MUTED)
    cbar.outline.set_visible(False)

    ax.set_title(title, color=_INK, fontsize=12, loc="left")
    fig.tight_layout()

    out = out_dir / filename
    fig.savefig(out, format="svg", metadata={"Date": None},
                bbox_inches="tight", pad_inches=0.12)
    plt.close(fig)
    return out
