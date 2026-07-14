from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.aggregate import group_mean
from aidmi_orchestrator.report.theme import apply_theme, color_for_cell, ordered_cells

_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"

# Below this mean f1 a strategy barely produces correct output, so "resource per
# unit quality" is a divide-by-almost-zero that would print a meaningless spike.
# Those cells are omitted from the ratio panels rather than dominating the axis.
_MIN_F1 = 0.05


def _cell_key(r):
    return r.cell


def _ratio(numerator, denominator):
    """Per-cell mean(numerator) / mean(f1), skipping all-fail cells."""
    return {
        c: numerator[c] / denominator[c]
        for c in numerator
        if c in denominator and denominator[c] >= _MIN_F1
    }


def _draw_bars(ax, cells, values, fmt, ylabel):
    present = [c for c in cells if c in values]
    xs = range(len(present))
    ys = [values[c] for c in present]
    colors = [color_for_cell(c) for c in present]
    if present:
        ax.bar(xs, ys, width=0.62, color=colors, zorder=3)
        for x, y in zip(xs, ys):
            ax.text(x, y, fmt.format(y), ha="center", va="bottom",
                    fontsize=8, color=_MUTED)
    ax.set_xticks(list(xs))
    ax.set_xticklabels(present, rotation=25, ha="right", fontsize=9, color=_INK)
    ax.set_ylabel(ylabel, color=_INK)
    ax.set_ylim(bottom=0)


def _ranked_by_f1(records):
    """Canonical STRATEGY_ORDER, shared with the bar section."""
    return ordered_cells({r.cell for r in records})


def fig_efficiency(records, out_dir) -> Path:
    """Resource cost of a unit of quality: $ per f1-point and tokens per f1-point.

    A mean-cost bar and a mean-f1 bar side by side still make you eyeball the
    trade-off; dividing them states it. tokens/f1 is the model-agnostic twin for
    runs where dollar cost is zero (local/self-hosted models).
    """
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = "aidmi-efficiency"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cells = _ranked_by_f1(records)
    f1 = group_mean(records, _cell_key, lambda r: r.f1)
    cost = group_mean(records, _cell_key, lambda r: r.cost)
    tokens = group_mean(records, _cell_key, lambda r: r.tokens_out)

    fig, (ax_cost, ax_tok) = plt.subplots(
        nrows=1, ncols=2, figsize=(max(11.0, 1.3 * len(cells) + 3.0), 4.4)
    )
    _draw_bars(ax_cost, cells, _ratio(cost, f1), "${:.3f}", "Cost per f1-point ($)")
    ax_cost.set_title("$ per unit quality", color=_INK, fontsize=11, loc="left")
    _draw_bars(ax_tok, cells, _ratio(tokens, f1), "{:.0f}", "Output tokens per f1-point")
    ax_tok.set_title("Tokens per unit quality", color=_INK, fontsize=11, loc="left")

    fig.suptitle(
        "Efficiency — resource per unit of quality (lower is better)",
        color=_INK, fontsize=12, x=0.02, ha="left",
    )
    fig.subplots_adjust(left=0.07, right=0.98, top=0.86, bottom=0.26, wspace=0.24)

    out = out_dir / "efficiency.svg"
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out


def fig_cost_drivers(records, out_dir) -> Path:
    """Two under-reported drivers of cost/reliability: retries and cache reuse.

    Retries are re-billed silent work; cache hits are the offsetting discount.
    Both already recorded, neither previously surfaced.
    """
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = "aidmi-cost-drivers"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cells = _ranked_by_f1(records)
    retries = group_mean(records, _cell_key, lambda r: r.retries)
    cache = group_mean(records, _cell_key, lambda r: r.cache_hit_rate)

    fig, (ax_r, ax_c) = plt.subplots(
        nrows=1, ncols=2, figsize=(max(11.0, 1.3 * len(cells) + 3.0), 4.4)
    )
    _draw_bars(ax_r, cells, retries, "{:.2f}", "Mean retries per run")
    ax_r.set_title("LLM retries", color=_INK, fontsize=11, loc="left")
    _draw_bars(ax_c, cells, cache, "{:.0%}", "Mean cache hit rate")
    ax_c.set_title("Prompt cache reuse", color=_INK, fontsize=11, loc="left")
    ax_c.set_ylim(0, 1.0)
    ax_c.yaxis.set_major_formatter(PercentFormatter(xmax=1.0))

    fig.suptitle(
        "Cost drivers — retries add billed work, cache hits discount it",
        color=_INK, fontsize=12, x=0.02, ha="left",
    )
    fig.subplots_adjust(left=0.07, right=0.98, top=0.86, bottom=0.26, wspace=0.24)

    out = out_dir / "cost_drivers.svg"
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out
