import json
from pathlib import Path

import dlt
from dlt.sources.filesystem import filesystem, read_jsonl

from aidmi_pipeline.config import MigrationRun, StagingConfig
from aidmi_pipeline.migration import run_migration

dlt.config["normalize.data_writer.disable_compression"] = True


def test_end_to_end(staging_db_url, tmp_path):
    fixture_dir = Path(__file__).parent / "fixtures"
    output_dir = tmp_path / "destination"

    source = (
        filesystem(
            bucket_url=fixture_dir.as_uri(),
            file_glob="source_contacts.jsonl",
        )
        | read_jsonl()
    ).with_name("contacts")

    run = MigrationRun(
        source=source,
        staging=StagingConfig(db_url=staging_db_url, dataset_name="staging"),
        target=dlt.destinations.filesystem(
            bucket_url=output_dir.as_uri(),
            layout="{table_name}.jsonl",
        ),
        target_dataset="target",
        target_tables=["users"],
        dbt_project_path=fixture_dir / "dbt_demo",
    )
    result = run_migration(run)

    assert result.transform.overall_status == "success"
    assert any(
        m.model_name == "users" and m.status == "success"
        for m in result.transform.models
    )

    output_path = output_dir / "target" / "users.jsonl"
    assert output_path.exists(), f"expected output at {output_path}"

    rows = {
        r["user_id"]: r
        for r in (json.loads(l) for l in output_path.read_text().splitlines() if l.strip())
    }

    # canonical: passes through, status normalized
    assert rows[1]["firstname"] == "John"
    assert rows[1]["status_enum"] == "active"

    # upper-cased name and status both normalized
    assert rows[2]["firstname"] == "Jane"
    assert rows[2]["status_enum"] == "active"

    # email trimmed, title-case status normalized
    assert rows[3]["email_address"] == "alice@example.com"
    assert rows[3]["status_enum"] == "active"

    # archived survives
    assert rows[4]["status_enum"] == "archived"

    # uppercase synonym normalized
    assert rows[5]["status_enum"] == "inactive"

    # nulls preserved (dlt's JSONL writer omits null fields; key-absent == null-preserved)
    assert rows[6].get("lastname") is None
    assert rows[6].get("email_address") is None

    # unknown status routes to 'unknown'
    assert rows[7]["status_enum"] == "unknown"

    # second canonical row
    assert rows[8]["firstname"] == "Frank"

    assert len(rows) == 8
