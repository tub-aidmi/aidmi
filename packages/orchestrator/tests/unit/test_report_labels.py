from pathlib import Path

from aidmi_orchestrator.report.data import campaign_labels


def _campaign(tmp_path: Path, cid: str, label: str | None) -> Path:
    d = tmp_path / cid
    d.mkdir()
    body = f"id: {cid}\n"
    if label is not None:
        body += f"label: {label}\n"
    (d / "campaign.yaml").write_text(body, encoding="utf-8")
    (d / "results.jsonl").write_text("", encoding="utf-8")
    return d


def test_labels_map_id_to_label(tmp_path):
    d = _campaign(tmp_path, "2026-07-06-yq3v", "v2-final-gemini")
    assert campaign_labels([d]) == {"2026-07-06-yq3v": "v2-final-gemini"}


def test_campaign_without_label_is_omitted(tmp_path):
    d = _campaign(tmp_path, "2026-07-06-yq3v", None)
    assert campaign_labels([d]) == {}


def test_missing_campaign_yaml_is_omitted(tmp_path):
    d = tmp_path / "loose"
    d.mkdir()
    (d / "results.jsonl").write_text("", encoding="utf-8")
    assert campaign_labels([d]) == {}


def test_results_jsonl_path_resolves_to_parent_campaign(tmp_path):
    d = _campaign(tmp_path, "2026-07-07-kw8r", "v2-final-ise")
    assert campaign_labels([d / "results.jsonl"]) == {"2026-07-07-kw8r": "v2-final-ise"}
