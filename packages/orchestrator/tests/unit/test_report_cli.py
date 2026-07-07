from typer.testing import CliRunner

from aidmi_orchestrator.cli import app


def test_report_cli_generates_gallery(tmp_path):
    runner = CliRunner()
    res = runner.invoke(
        app,
        [
            "report",
            "packages/orchestrator/tests/unit/fixtures/mini_results.jsonl",
            "--out",
            str(tmp_path / "rep"),
        ],
    )
    assert res.exit_code == 0
    assert (tmp_path / "rep" / "index.html").exists()
