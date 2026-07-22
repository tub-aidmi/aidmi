from aidmi_orchestrator.provenance import dbt_project_sha256, file_sha256


def test_file_sha256_stable(tmp_path):
    p = tmp_path / "a.txt"
    p.write_text("hello", encoding="utf-8")
    h1 = file_sha256(p)
    h2 = file_sha256(p)
    assert h1 == h2
    assert len(h1) == 64


def test_dbt_project_sha256_none_when_missing(tmp_path):
    assert dbt_project_sha256(tmp_path / "nope") is None


def test_dbt_project_sha256_from_models(tmp_path):
    project = tmp_path / "dbt"
    models = project / "models"
    models.mkdir(parents=True)
    (models / "a.sql").write_text("SELECT 1", encoding="utf-8")
    (models / "b.sql").write_text("SELECT 2", encoding="utf-8")
    h = dbt_project_sha256(project)
    assert h is not None
    assert len(h) == 64
