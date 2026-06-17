from aidmi_orchestrator.report.base import MetricDescriptor, PlotScope, register_report_contributor


class HarnessContributor:
    name = "harness"

    def metrics(self) -> list[MetricDescriptor]:
        g = frozenset({PlotScope.GLOBAL})
        return [
            MetricDescriptor("ran_ok", "rate", headline=True, plot_scopes=g, vmin=0.0, vmax=1.0),
            MetricDescriptor("wall_clock_seconds", "duration", headline=True, plot_scopes=g),
        ]


register_report_contributor("harness", HarnessContributor)
