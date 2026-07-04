from aidmi_orchestrator.report.base import MetricDescriptor, PlotScope, register_report_contributor


class GroundTruthContributor:
    name = "ground_truth"

    def metrics(self) -> list[MetricDescriptor]:
        g = frozenset({PlotScope.GLOBAL})
        return [
            MetricDescriptor("gt_recall_overall", "rate", headline=True, plot_scopes=g, vmin=0.0, vmax=1.0),
            MetricDescriptor("gt_field_accuracy_overall", "rate", headline=True, plot_scopes=g, vmin=0.0, vmax=1.0),
            MetricDescriptor("gt_tables_materialized", "rate", headline=True, plot_scopes=g, vmin=0.0, vmax=1.0),
            MetricDescriptor("gt_f1_overall", "rate", headline=False, plot_scopes=g, vmin=0.0, vmax=1.0),
        ]


register_report_contributor("ground_truth", GroundTruthContributor)
