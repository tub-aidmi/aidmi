from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.aggregate import group_mean
from aidmi_orchestrator.report.theme import apply_theme

_INK = "#0b0b0b"
_MUTED = "#898781"
_SURFACE = "#fcfcfb"

# Reasoning tokens are billed and add latency but never appear in the output the
# pipeline is scored on. On the gemini run they are 34-44% of every strategy's
# output budget -- a cost the mean-cost bar folds in without ever naming. Split
# the visible-output bar from the reasoning bar so that tax is explicit.
_VISIBLE_COLOR = "#2a78d6"
_THOUGHT_COLOR = "#B279A2"


def _cell_key(r):
    return r.cell


def fig_thinking_tokens(records, out_dir) -> Path:
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    apply_theme()
    mpl.rcParams["svg.hashsalt"] = "aidmi-thinking-tokens"
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    out_tokens = group_mean(records, _cell_key, lambda r: r.tokens_out)
    thoughts = group_mean(records, _cell_key, lambda r: r.tokens_thoughts)

    cells = [c for c in sorted(out_tokens) if c in out_tokens]
    # Visible output = total output minus reasoning; guard against negative if a
    # provider ever reports thoughts outside the output total.
    visible = [max(0.0, out_tokens[c] - thoughts.get(c, 0.0)) for c in cells]
    thought = [thoughts.get(c, 0.0) for c in cells]

    fig, ax = plt.subplots(figsize=(max(8.0, 1.2 * len(cells) + 2.5), 4.6))
    xs = range(len(cells))
    ax.bar(
        xs, visible, width=0.62, color=_VISIBLE_COLOR, zorder=3, label="Visible output"
    )
    ax.bar(
        xs,
        thought,
        width=0.62,
        bottom=visible,
        color=_THOUGHT_COLOR,
        zorder=3,
        label="Reasoning (thoughts)",
    )

    for i, _c in enumerate(cells):
        total = visible[i] + thought[i]
        if total > 0:
            pct = 100 * thought[i] / total
            ax.text(
                i,
                total,
                f"{pct:.0f}%",
                ha="center",
                va="bottom",
                fontsize=8,
                color=_MUTED,
            )

    ax.set_xticks(list(xs))
    ax.set_xticklabels(cells, rotation=25, ha="right", fontsize=9, color=_INK)
    ax.set_ylabel("Mean output tokens per run", color=_INK)
    ax.set_ylim(bottom=0)
    ax.set_title(
        "Reasoning-token tax by strategy (label = reasoning share of output)",
        color=_INK,
        fontsize=12,
        loc="left",
    )
    ax.legend(
        handles=[
            Patch(facecolor=_VISIBLE_COLOR, label="Visible output"),
            Patch(facecolor=_THOUGHT_COLOR, label="Reasoning (thoughts)"),
        ],
        loc="upper right",
        labelcolor=_INK,
    )
    fig.tight_layout()

    out = out_dir / "thinking_tokens.svg"
    fig.savefig(out, format="svg", metadata={"Date": None})
    plt.close(fig)
    return out
