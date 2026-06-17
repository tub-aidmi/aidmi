"""Report types, metric descriptors, and contributor registry."""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, Protocol, runtime_checkable

MetricKind = Literal["rate", "count", "duration", "tokens", "cost"]


class PlotScope(StrEnum):
    GLOBAL = "global"
    BY_STRATEGY = "by_strategy"
    BY_MODEL = "by_model"


class PlotKind(StrEnum):
    HEATMAP = "heatmap"
    DISTRIBUTION = "distribution"
    STACKED_BAR = "stacked_bar"


@dataclass(frozen=True)
class MetricDescriptor:
    key: str
    kind: MetricKind
    headline: bool = False
    plot_scopes: frozenset[PlotScope] = frozenset()
    lower_is_better: bool = False
    vmin: float | None = None
    vmax: float | None = None


@dataclass(frozen=True)
class PlotRecipe:
    scope: PlotScope
    kind: PlotKind
    metric: str


@runtime_checkable
class ReportContributor(Protocol):
    name: str

    def metrics(self) -> list[MetricDescriptor]: ...


_CONTRIBUTORS: dict[str, type] = {}


def register_report_contributor(name: str, cls: type) -> None:
    if name in _CONTRIBUTORS:
        raise ValueError(f"report contributor {name!r} already registered")
    _CONTRIBUTORS[name] = cls


def list_report_contributors() -> list[str]:
    return sorted(_CONTRIBUTORS)


def all_metric_descriptors() -> list[MetricDescriptor]:
    out: list[MetricDescriptor] = []
    for name in list_report_contributors():
        out.extend(_CONTRIBUTORS[name]().metrics())
    return out
