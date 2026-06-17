from aidmi_orchestrator.report.base import MetricDescriptor, PlotScope, register_report_contributor


class RowEqualityContributor:
    name = "row_equality"

    def metrics(self) -> list[MetricDescriptor]:
        g = frozenset({PlotScope.GLOBAL})
        return [
            MetricDescriptor("row_count_match", "rate", headline=True, plot_scopes=g, vmin=0.0, vmax=1.0),
        ]


register_report_contributor("row_equality", RowEqualityContributor)
