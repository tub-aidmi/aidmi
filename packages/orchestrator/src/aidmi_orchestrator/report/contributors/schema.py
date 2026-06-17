from aidmi_orchestrator.report.base import MetricDescriptor, PlotScope, register_report_contributor


class SchemaContributor:
    name = "schema"

    def metrics(self) -> list[MetricDescriptor]:
        g = frozenset({PlotScope.GLOBAL})
        return [
            MetricDescriptor("target_columns_covered", "rate", headline=True, plot_scopes=g, vmin=0.0, vmax=1.0),
            MetricDescriptor("type_mismatches", "count", headline=True, plot_scopes=g, lower_is_better=True),
            MetricDescriptor("extraneous_columns", "count", headline=True, plot_scopes=g, lower_is_better=True),
        ]


register_report_contributor("schema", SchemaContributor)
