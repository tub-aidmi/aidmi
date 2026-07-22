from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.aggregate import group_mean
from aidmi_orchestrator.report.theme import apply_theme, ordered_cells

_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"

# live_query_tool feeds live query results back into the prompt, so its whole
# cost lives in input tokens; metadata_only never sees the rows. The question
# this figure answers is whether that ~2.5x token spend buys any quality -- so
# ctx is the categorical axis (two colored bars per strategy), token usage on
# one panel and quality on the other, read side by side.
_CTX_ORDER = ["metadata_only", "live_query_tool"]
_CTX_LABELS = {"metadata_only": "metadata_only", "live_query_tool": "live_query_tool"}
_CTX_COLORS = {"metadata_only": "#2a78d6", "live_query_tool": "#F58518"}


def _cell_key(r):
    return r.cell


def _cell_ctx_key(r):
    return (r.cell, r.ctx)


def _f1_outcome(r):
    return r.f1 if r.f1 is not None else 0.0


def _total_tokens(r):
    if r.tokens_in is None and r.tokens_out is None:
        return None
    return (r.tokens_in or 0) + (r.tokens_out or 0)


def _ranked_cells(records):
    """Canonical STRATEGY_ORDER, shared with the bar section."""
    return ordered_cells({r.cell for r in records})


def _draw_grouped(ax, cells, per_cell_ctx, fmt, ylabel, *, ctxs):
    width = 0.8 / max(1, len(ctxs))
    for k, ctx in enumerate(ctxs):
        offset = (k - (len(ctxs) - 1) / 2) * width
        xs, ys = [], []
        for i, c in enumerate(cells):
            v = per_cell_ctx.get((c, ctx))
            if v is not None:
                xs.append(i + offset)
                ys.append(v)
        if xs:
            ax.bar(
                xs,
                ys,
                width=width * 0.9,
                color=_CTX_COLORS.get(ctx, _MUTED),
                zorder=3,
                label=_CTX_LABELS.get(ctx, ctx),
            )
            for x, y in zip(xs, ys, strict=False):
                ax.text(
                    x,
                    y,
                    fmt.format(y),
                    ha="center",
                    va="bottom",
                    fontsize=7.5,
                    color=_MUTED,
                )
    ax.set_xlim(-0.5, len(cells) - 0.5)
    ax.set_xticks(range(len(cells)))
    ax.set_xticklabels(cells, rotation=25, ha="right", fontsize=9, color=_INK)
    ax.set_ylabel(ylabel, color=_INK)
    ax.set_ylim(bottom=0)


def fig_ctx_comparison(records, out_dir) -> Path:
    """live_query_tool vs metadata_only: token usage next to the quality it buys.

    The aggregate story is a ~2.5x input-token spend for a fraction of a point
    of f1; splitting per strategy exposes whether any single strategy actually
    converts the extra context into quality, which a pooled mean would hide.
    """
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
    from matplotlib.ticker import PercentFormatter

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = "aidmi-ctx-comparison"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ctxs = [c for c in _CTX_ORDER if any(r.ctx == c for r in records)]
    cells = _ranked_cells(records)
    models = sorted({r.model for r in records})
    rows = models if len(models) > 1 else [None]

    fig, axes = plt.subplots(
        nrows=len(rows),
        ncols=2,
        squeeze=False,
        figsize=(max(11.0, 1.4 * len(cells) + 3.0), 3.9 * len(rows) + 0.8),
    )

    for i, model in enumerate(rows):
        ax_tok, ax_qual = axes[i][0], axes[i][1]
        subset = records if model is None else [r for r in records if r.model == model]
        tokens = group_mean(subset, _cell_ctx_key, _total_tokens)
        f1 = group_mean(subset, _cell_ctx_key, _f1_outcome)

        _draw_grouped(
            ax_tok, cells, tokens, "{:.0f}", "Mean tokens/run (in+out)", ctxs=ctxs
        )
        ax_tok.set_title("Token usage", color=_INK, fontsize=11, loc="left")
        _draw_grouped(ax_qual, cells, f1, "{:.2f}", "Mean f1 (null = 0)", ctxs=ctxs)
        ax_qual.set_title("Quality", color=_INK, fontsize=11, loc="left")
        ax_qual.set_ylim(0, 1.08)
        ax_qual.yaxis.set_major_formatter(PercentFormatter(xmax=1.0))

        if model is not None:
            ax_tok.set_title(
                f"{model} — Token usage", color=_INK, fontsize=11, loc="left"
            )

    handles = [Patch(facecolor=_CTX_COLORS[c], label=_CTX_LABELS[c]) for c in ctxs]
    fig.legend(
        handles=handles,
        loc="upper right",
        bbox_to_anchor=(0.98, 1.0),
        ncol=len(handles),
        labelcolor=_INK,
        frameon=False,
    )

    fig.suptitle(
        "Context mode — extra tokens vs the quality they buy",
        color=_INK,
        fontsize=12,
        x=0.02,
        ha="left",
        y=0.995,
    )
    fig.subplots_adjust(
        left=0.07,
        right=0.98,
        top=0.84,
        bottom=0.30 / len(rows) + 0.08,
        wspace=0.22,
        hspace=0.75,
    )

    out = out_dir / "ctx_comparison.svg"
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out
