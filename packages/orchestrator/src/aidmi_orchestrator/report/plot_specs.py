"""Plot specification dataclasses for report renderers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Union

import numpy as np

from aidmi_orchestrator.report.base import MetricDescriptor

if TYPE_CHECKING:
    from aidmi_orchestrator.report.role_aggregate import RoleStackedBarSpec


@dataclass
class StrategyDistributionPlotSpec:
    fixture: str
    strategy: str
    metric: str
    col_labels: list[str]
    values_by_model: list[list[float]]


@dataclass
class FunnelPlotSpec:
    fixture: str
    strategy: str
    stage_labels: list[str]
    col_labels: list[str]
    pass_rates: list[list[float]]
    n_by_model: list[int]


@dataclass
class GroupedBarPlotSpec:
    fixture: str
    strategy: str
    plot_id: str
    series_labels: list[str]
    col_labels: list[str]
    values: list[list[float]]
    n_by_model: list[int]


@dataclass
class DumbbellPlotSpec:
    fixture: str
    metric: str
    base_label: str
    variant_label: str
    col_labels: list[str]
    base_values: list[float]
    variant_values: list[float]
    n_by_model: list[int]


@dataclass
class TableModelHeatmapPlotSpec:
    fixture: str
    strategy: str
    metric: str
    row_labels: list[str]
    col_labels: list[str]
    values: np.ndarray
    std: np.ndarray
    n: np.ndarray


@dataclass
class HeatmapPlotSpec:
    fixture: str
    metric: str
    row_labels: list[str]
    col_labels: list[str]
    values: np.ndarray
    descriptor: MetricDescriptor
    std: np.ndarray
    n: np.ndarray


@dataclass
class DistributionPlotSpec:
    fixture: str
    metric: str
    group_labels: list[str]
    values_by_group: list[list[float]]
    descriptor: MetricDescriptor


PlotSpec = Union[
    "HeatmapPlotSpec",
    "DistributionPlotSpec",
    "RoleStackedBarSpec",
    "StrategyDistributionPlotSpec",
    "FunnelPlotSpec",
    "GroupedBarPlotSpec",
    "DumbbellPlotSpec",
    "TableModelHeatmapPlotSpec",
]
