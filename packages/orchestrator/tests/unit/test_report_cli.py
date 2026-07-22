import re
from pathlib import Path

from typer.testing import CliRunner

from aidmi_orchestrator.cli import app
from aidmi_orchestrator.report.data import load_records

FIX = Path("packages/orchestrator/tests/unit/fixtures/mini_results.jsonl")


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


def test_report_cli_uses_campaign_label_from_campaign_yaml(tmp_path):
    campaign_id = load_records([FIX])[0].campaign
    camp_dir = tmp_path / "camp"
    camp_dir.mkdir()
    (camp_dir / "results.jsonl").write_text(
        FIX.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (camp_dir / "campaign.yaml").write_text(
        f"id: {campaign_id}\nlabel: My Campaign\n", encoding="utf-8"
    )

    runner = CliRunner()
    res = runner.invoke(
        app,
        ["report", str(camp_dir), "--out", str(tmp_path / "rep")],
    )
    assert res.exit_code == 0

    html = (tmp_path / "rep" / "index.html").read_text()
    title = re.search(r"<title>(.*?)</title>", html).group(1)
    assert "My Campaign" in title
