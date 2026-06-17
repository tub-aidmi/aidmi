from aidmi_orchestrator.report.base import MetricDescriptor, PlotScope, register_report_contributor


class ExecutionContributor:
    name = "execution"

    def metrics(self) -> list[MetricDescriptor]:
        g = frozenset({PlotScope.GLOBAL})
        return [
            MetricDescriptor("dbt_success", "rate", headline=True, plot_scopes=g, vmin=0.0, vmax=1.0),
        ]


register_report_contributor("execution", ExecutionContributor)
