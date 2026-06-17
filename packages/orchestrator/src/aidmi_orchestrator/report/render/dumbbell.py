"""Self-correction dumbbell plot rendering."""
from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.plot_specs import DumbbellPlotSpec


def render_dumbbell_svg(spec: DumbbellPlotSpec, svg_path: Path) -> None:
    try:
        import matplotlib
    except ImportError as e:
        raise RuntimeError(
            "matplotlib is not installed — install the plots extra: "
            "uv sync --extra plots (or pip install 'aidmi-orchestrator[plots]')"
        ) from e
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    n = len(spec.col_labels)
    if n == 0:
        return

    fig, ax = plt.subplots(
        figsize=(max(6.0, n * 1.4), 4.5),
        constrained_layout=True,
    )
    x = list(range(n))
    for i in x:
        ax.plot(
            [i, i],
            [spec.base_values[i], spec.variant_values[i]],
            color="#888888",
            linewidth=2,
            zorder=1,
        )
    ax.scatter(x, spec.base_values, color="#4C78A8", s=60, label=spec.base_label, zorder=2)
    ax.scatter(x, spec.variant_values, color="#F58518", s=60, label=spec.variant_label, zorder=2)

    ax.set_xticks(x)
    ax.set_xticklabels(spec.col_labels, rotation=35, ha="right")
    ax.set_xlabel("Model")
    ax.set_ylabel(spec.metric)
    ax.set_title(
        f"Self-correction — {spec.metric} — {spec.base_label} vs {spec.variant_label} — {spec.fixture}"
    )
    ax.legend(loc="upper right")

    svg_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(svg_path, format="svg", bbox_inches="tight", pad_inches=0.1)
    plt.close(fig)
