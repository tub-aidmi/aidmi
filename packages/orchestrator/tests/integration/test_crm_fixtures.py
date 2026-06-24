"""Smoke test: the crm_master sql_database source loads and discover sees its tables."""
import asyncio
from pathlib import Path

from aidmi_pipeline.config import MigrationRun, StagingConfig
from aidmi_pipeline.migration import extract_source

import aidmi_orchestrator.fixtures  # noqa: F401 — triggers registration
from aidmi_orchestrator.fixtures.base import get_fixture
from aidmi_orchestrator.discover import discover


def test_crm_master_source_loads_and_discovers(staging_db_url, tmp_path):
    fixture = get_fixture("crm_master")
    run_id = "crmtest0001"
    staging = StagingConfig.for_run(staging_db_url, run_id)
    pipeline_run = MigrationRun(
        source=fixture.source_factory(),
        staging=staging,
        target=None,
        target_dataset="",
        target_tables=[],
        dbt_project_path=tmp_path / "dbt_project",
    )
    extract_result = asyncio.run(asyncio.to_thread(extract_source, pipeline_run))
    assert extract_result.rows_extracted > 0

    summary = discover(staging.db_url, staging.raw_dataset_name, samples_per_table=5)
    by_name = {t.name: t for t in summary.tables}
    expected = {
        "master_assets": 900, "master_kontakte": 497, "master_kunden": 210,
        "master_opportunities": 625, "master_projekte": 342,
    }
    for tname, count in expected.items():
        assert tname in by_name, f"missing table {tname}; got {sorted(by_name)}"
        assert by_name[tname].row_count == count
    assert by_name["master_kunden"].sample_rows
