"""Rep stability distribution plot rendering."""
from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.plot_specs import StrategyDistributionPlotSpec


def render_distribution_svg(spec: StrategyDistributionPlotSpec, svg_path: Path) -> None:
    try:
        import matplotlib
    except ImportError as e:
        raise RuntimeError(
            "matplotlib is not installed — install the plots extra: "
            "uv sync --extra plots (or pip install 'aidmi-orchestrator[plots]')"
        ) from e
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    n_models = len(spec.col_labels)
    if n_models == 0:
        return

    fig, ax = plt.subplots(
        figsize=(max(6.0, n_models * 1.4), 4.5),
        constrained_layout=True,
    )
    positions = list(range(1, n_models + 1))
    bp = ax.boxplot(
        spec.values_by_model,
        positions=positions,
        widths=0.5,
        patch_artist=True,
        showfliers=False,
    )
    for patch in bp["boxes"]:
        patch.set_facecolor("#c6dbef")

    for i, values in enumerate(spec.values_by_model):
        jitter = [(i + 1) + (j - len(values) / 2) * 0.05 for j in range(len(values))]
        ax.scatter(jitter, values, color="#08519c", s=20, zorder=3, alpha=0.8)

    ax.set_xticks(positions)
    ax.set_xticklabels(spec.col_labels, rotation=35, ha="right")
    ax.set_xlabel("Model")
    ax.set_ylabel(spec.metric)
    ax.set_title(f"Rep stability — {spec.metric} — {spec.strategy} — {spec.fixture}")

    svg_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(svg_path, format="svg", bbox_inches="tight", pad_inches=0.1)
    plt.close(fig)
