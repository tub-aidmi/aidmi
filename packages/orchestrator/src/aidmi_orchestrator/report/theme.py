from __future__ import annotations
import re

import matplotlib as mpl

_VERSION_RE = re.compile(r"^(?P<base>.+?)(?P<ver>_v\d+)$")

# Single source of truth for strategy identity: this list fixes BOTH the
# left-to-right / top-to-bottom order strategies appear in on every plot that
# is not explicitly performance-ranked, AND (paired with STRATEGY_COLORS) their
# color. Reorder here to reorder everywhere; recolor in STRATEGY_COLORS.
STRATEGY_ORDER = [
    "ensemble_vote",
    "plan_then_execute",
    "write_then_critique",
    "plan_write_critique",
    "structured_per_table",
    "write_tools_freeform",
    "write_tools_freeform_inlinedbt",
]

# Brand palette. ensemble_vote (top performer) carries the primary brand red,
# plan_then_execute (best trade-off) the secondary orange. write_tools_freeform
# and its inlinedbt sibling share a cool blue family, set apart from the warm
# brand cluster. Middle three take distinct hues for clean 7-way separation.
STRATEGY_COLORS = {
    "ensemble_vote": "#c40d1e",
    "plan_then_execute": "#ff6c00",
    "write_then_critique": "#e8a800",
    "plan_write_critique": "#4f9d2e",
    "structured_per_table": "#b5297a",
    "write_tools_freeform": "#2f6fb0",
    "write_tools_freeform_inlinedbt": "#7fb0dd",
}

# Canonical fixture order, hardest -> easiest, matched version-agnostically
# (master_v2 ranks as master). Ranking rationale: master is the coverage floor
# (lowest recall/mat rate), missing_relations the correctness floor (lowest
# field accuracy), then messy_data, then wrong_field_names (top on every axis).
# Unknown fixtures trail, sorted by name.
FIXTURE_ORDER = [
    "master",
    "missing_relations",
    "messy_data",
    "wrong_field_names",
]

# Strategies dropped from the whole report (every figure/table/order derives
# from the filtered record set). Comment a line out to bring one back; the CLI
# --exclude flag adds to this set for one-off runs.
EXCLUDED_STRATEGIES = {
    "plan_write_critique",
    "write_tools_freeform_inlinedbt",
}

MODEL_MARKERS = {"gemini25flash": "o", "qwen35b": "s", "mistral128b": "^"}
_FALLBACK_C = "#888888"
_FALLBACK_M = "D"

# Single brand sequential ramp (near-white -> orange -> red -> deep maroon),
# used by every heatmap regardless of metric. Both brand colors sit inside it
# (#ff6c00 mid, #c40d1e upper).
BRAND_ORRD = [
    "#fff7f2",
    "#fddccb",
    "#fcbfa0",
    "#fb9d6b",
    "#ff7a1f",
    "#ff6c00",
    "#e8471a",
    "#c40d1e",
    "#9c0a17",
    "#6f040d",
]


def strip_common_version(labels):
    """Drop a trailing _v<N> suffix from labels only when every label carries
    the same one (e.g. all *_v2 -> bare names); otherwise leave them untouched.
    Order preserved. Non-versioned labels (strategy cells) pass through."""
    labels = list(labels)
    matches = [_VERSION_RE.match(l) for l in labels]
    if labels and all(matches):
        versions = {m.group("ver") for m in matches}
        if len(versions) == 1:
            return [m.group("base") for m in matches]
    return labels


def color_for_cell(cell):
    return STRATEGY_COLORS.get(cell, _FALLBACK_C)


def marker_for_model(model):
    return MODEL_MARKERS.get(model, _FALLBACK_M)


def ordered_cells(cells):
    """Strategies in canonical STRATEGY_ORDER; any unknown cell trails, sorted."""
    present = set(cells)
    ranked = [c for c in STRATEGY_ORDER if c in present]
    extra = sorted(present - set(STRATEGY_ORDER))
    return ranked + extra


def cells_covering_states(records, attr, states):
    """Cells with at least one run in every one of `states` for `attr`.

    A lever plot compares a metric across the lever's states; a strategy that was
    only ever run in one state (e.g. self-correction always on) has no comparison
    to make and would render as a lone point, so it is dropped from that plot.
    """
    seen: dict[str, set] = {}
    for r in records:
        value = getattr(r, attr)
        if value in states:
            seen.setdefault(r.cell, set()).add(value)
    need = set(states)
    return {cell for cell, got in seen.items() if need <= got}


def _fixture_base(fixture):
    m = _VERSION_RE.match(fixture)
    return m.group("base") if m else fixture


def ordered_fixtures(fixtures):
    """Fixtures in canonical FIXTURE_ORDER (matched on the version-stripped base
    name); any unknown fixture trails, sorted. Order preserved for ties."""
    rank = {name: i for i, name in enumerate(FIXTURE_ORDER)}
    return sorted(
        set(fixtures),
        key=lambda f: (rank.get(_fixture_base(f), len(FIXTURE_ORDER)), f),
    )


def sequential_cmap():
    from matplotlib.colors import LinearSegmentedColormap

    return LinearSegmentedColormap.from_list("aidmi_brand_orrd", BRAND_ORRD)


def apply_theme():
    mpl.rcParams.update(
        {
            "figure.figsize": (7, 4.5),
            "figure.dpi": 100,
            "svg.fonttype": "none",
            "font.size": 11,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "grid.alpha": 0.3,
            "axes.axisbelow": True,
            "legend.frameon": False,
            "figure.facecolor": "none",
            "axes.facecolor": "none",
            "savefig.facecolor": "none",
            "savefig.edgecolor": "none",
        }
    )
