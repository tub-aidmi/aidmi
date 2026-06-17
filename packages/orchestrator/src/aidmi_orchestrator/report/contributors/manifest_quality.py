from aidmi_orchestrator.report.base import MetricDescriptor, register_report_contributor


class ManifestQualityContributor:
    name = "manifest_quality"

    def metrics(self) -> list[MetricDescriptor]:
        return [
            MetricDescriptor("manifest_present", "rate", headline=True),
            MetricDescriptor("manifest_table_coverage", "rate", headline=True),
            MetricDescriptor("manifest_column_coverage", "rate", headline=True),
        ]


register_report_contributor("manifest_quality", ManifestQualityContributor)
