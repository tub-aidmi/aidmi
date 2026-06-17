from aidmi_orchestrator.report.base import MetricDescriptor, PlotScope, register_report_contributor


class LlmUsageContributor:
    name = "llm_usage"

    def metrics(self) -> list[MetricDescriptor]:
        g = frozenset({PlotScope.GLOBAL})
        return [
            MetricDescriptor("llm_calls_total", "count", headline=True, plot_scopes=g),
            MetricDescriptor("tokens_input_total", "tokens", headline=True, plot_scopes=g),
            MetricDescriptor("tokens_output_total", "tokens", headline=True, plot_scopes=g),
            MetricDescriptor("dollar_cost_total", "cost", headline=True, plot_scopes=g),
        ]


register_report_contributor("llm_usage", LlmUsageContributor)
