from aidmi_orchestrator.report.base import MetricDescriptor, PlotScope, register_report_contributor


class DataPreservationContributor:
    name = "data_preservation"

    def metrics(self) -> list[MetricDescriptor]:
        g = frozenset({PlotScope.GLOBAL})
        return [
            MetricDescriptor(
                "preservation_row_ratio_mean", "rate", headline=True, plot_scopes=g, vmin=0.0, vmax=1.0,
            ),
            MetricDescriptor(
                "preservation_null_inflation_mean", "rate", headline=True, plot_scopes=g, lower_is_better=True,
            ),
            MetricDescriptor(
                "preservation_distinct_ratio_mean", "rate", headline=True, plot_scopes=g, vmin=0.0, vmax=1.0,
            ),
            MetricDescriptor("preservation_empty_tables", "count", headline=True),
        ]


register_report_contributor("data_preservation", DataPreservationContributor)
