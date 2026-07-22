import datetime as dt
import hashlib
from pathlib import Path

import pytest

from aidmi_orchestrator.scripts import build_fixtures, verify_fixtures
from aidmi_orchestrator.scripts.verify_fixtures import (
    FROZEN_TODAY,
    digest_tree,
    format_manifest,
    frozen_today,
    guarded_generate,
    parse_manifest,
    verify,
)


def _write(root: Path, rel: str, text: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def test_digest_tree_hashes_files_by_relative_path(tmp_path: Path):
    _write(tmp_path, "master_v2/source.sql", "select 1;")
    digests = digest_tree(tmp_path)
    assert digests == {"master_v2/source.sql": hashlib.sha256(b"select 1;").hexdigest()}


def test_format_manifest_is_sorted_two_space_separated(tmp_path: Path):
    text = format_manifest({"b/x.sql": "22", "a/y.sql": "11"})
    assert text == "11  a/y.sql\n22  b/x.sql\n"


def test_manifest_round_trips(tmp_path: Path):
    _write(tmp_path, "a/source.sql", "one")
    _write(tmp_path, "b/destination.sql", "two")
    digests = digest_tree(tmp_path)
    assert parse_manifest(format_manifest(digests)) == digests


def test_verify_clean_run_returns_no_drift(tmp_path: Path):
    baseline = tmp_path / "fixtures.sha256"
    digests = {"a/source.sql": "aa", "b/source.sql": "bb"}
    baseline.write_text(format_manifest(digests))

    assert verify(digests, baseline) == []


def test_verify_reports_content_drift(tmp_path: Path):
    baseline = tmp_path / "fixtures.sha256"
    baseline.write_text(format_manifest({"a/source.sql": "a" * 64}))

    drift = verify({"a/source.sql": "b" * 64}, baseline)
    assert len(drift) == 1
    assert "a/source.sql" in drift[0]
    assert "content differs" in drift[0]


def test_verify_reports_missing_and_added_files(tmp_path: Path):
    baseline = tmp_path / "fixtures.sha256"
    baseline.write_text(format_manifest({"a/source.sql": "aa", "b/source.sql": "bb"}))

    drift = verify({"a/source.sql": "aa", "c/source.sql": "cc"}, baseline)
    assert any("b/source.sql: in baseline but not regenerated" == d for d in drift)
    assert any("c/source.sql: regenerated but not in baseline" == d for d in drift)


def test_verify_missing_baseline_reports_actionable_drift(tmp_path: Path):
    baseline = tmp_path / "no-such-baseline.sha256"
    drift = verify({"a/source.sql": "aa"}, baseline)

    assert len(drift) == 1
    assert "--snapshot" in drift[0]
    assert str(baseline) in drift[0]


def test_frozen_today_pins_relative_date_strings():
    from faker.providers.date_time import Provider

    with frozen_today():
        assert Provider._parse_date("today") == FROZEN_TODAY
        assert Provider._parse_date("+1y") == FROZEN_TODAY + dt.timedelta(days=365)
        assert Provider._parse_date(dt.date(2020, 1, 1)) == dt.date(2020, 1, 1)

    assert Provider._parse_date("today") == dt.date.today()


def test_guarded_generate_raises_if_repo_fixtures_are_written(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    fixtures_dir = tmp_path / "fixtures"
    fixtures_dir.mkdir()
    monkeypatch.setattr(build_fixtures, "FIXTURES_DIR", fixtures_dir)
    canary = fixtures_dir / "_guard_test_canary.sql"

    def fake_generate(out_root: Path) -> None:
        canary.write_text("this should never land in the repo")

    monkeypatch.setattr(verify_fixtures, "generate", fake_generate)

    with pytest.raises(RuntimeError, match="_guard_test_canary.sql"):
        guarded_generate(tmp_path / "out")
