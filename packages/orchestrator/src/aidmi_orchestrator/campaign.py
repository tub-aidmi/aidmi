"""Campaign directories: id generation, active pointer, layout resolution."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from ulid import ULID

from aidmi_orchestrator.domain import CampaignProvenance
from aidmi_orchestrator.provenance import make_campaign_provenance

DEFAULT_BENCHMARKS_ROOT = Path("benchmarks")


def make_campaign_id() -> str:
    date = datetime.utcnow().strftime("%Y-%m-%d")
    suffix = str(ULID())[-4:].lower()
    return f"{date}-{suffix}"


def campaign_dir(campaign_id: str, root: Path = DEFAULT_BENCHMARKS_ROOT) -> Path:
    return root / campaign_id


class Campaign:
    def __init__(self, path: Path):
        self.path = path.resolve()
        self.id = self.path.name

    @property
    def results_jsonl(self) -> Path:
        return self.path / "results.jsonl"

    @property
    def runs_dir(self) -> Path:
        return self.path / "runs"

    @property
    def grid_yaml(self) -> Path:
        return self.path / "grid.yaml"

    @property
    def campaign_yaml(self) -> Path:
        return self.path / "campaign.yaml"

    def ensure_layout(self) -> None:
        self.path.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def create(
        cls, label: str | None = None, root: Path = DEFAULT_BENCHMARKS_ROOT
    ) -> Campaign:
        cid = make_campaign_id()
        camp = cls(campaign_dir(cid, root))
        camp.ensure_layout()
        meta = make_campaign_provenance(campaign_id=cid, label=label or None)
        camp.campaign_yaml.write_text(
            yaml.safe_dump(meta.model_dump(mode="json"), sort_keys=False),
            encoding="utf-8",
        )
        return camp

    @classmethod
    def load(cls, campaign_id: str, root: Path = DEFAULT_BENCHMARKS_ROOT) -> Campaign:
        path = campaign_dir(campaign_id, root)
        if not path.is_dir():
            raise FileNotFoundError(f"campaign not found: {path}")
        return cls(path)

    @classmethod
    def load_path(cls, path: Path) -> Campaign:
        if not path.is_dir():
            raise FileNotFoundError(f"campaign not found: {path}")
        return cls(path)


def resolve_campaign(
    campaign: str | Path,
    root: Path = DEFAULT_BENCHMARKS_ROOT,
) -> Campaign:
    path = Path(campaign)
    if path.is_dir():
        return Campaign.load_path(path)
    return Campaign.load(str(campaign), root)


def results_jsonl_for_campaign(campaign_path: Path) -> Path | None:
    """Return results.jsonl path for campaign root or legacy layout."""
    direct = campaign_path / "results.jsonl"
    if direct.is_file():
        return direct
    legacy = campaign_path / "results" / "results.jsonl"
    if legacy.is_file():
        return legacy
    return None


def bundle_dir_for_run(campaign_path: Path, run_id: str, rep_index: int = 0) -> Path:
    name = run_id if rep_index == 0 else f"{run_id}_rep{rep_index}"
    return campaign_path / "runs" / name


def resolve_run_bundle(campaign_path: Path, run_id: str, rep_index: int = 0) -> Path:
    """Resolve run bundle directory; supports legacy dbt-only layout."""
    bundle = bundle_dir_for_run(campaign_path, run_id, rep_index)
    if (bundle / "result.json").is_file():
        return bundle
    if (bundle / "dbt_project").is_dir():
        return bundle

    legacy_dbt = campaign_path / "results" / "dbt" / run_id
    if (legacy_dbt / "dbt_project").is_dir():
        return legacy_dbt

    legacy_dbt_flat = campaign_path / "dbt" / run_id
    if (legacy_dbt_flat / "dbt_project").is_dir():
        return legacy_dbt_flat

    raise FileNotFoundError(
        f"run bundle not found for {run_id!r} under {campaign_path} "
        f"(tried runs/{run_id}, results/dbt/{run_id})"
    )


def resolve_dbt_project(campaign_path: Path, run_id: str, rep_index: int = 0) -> Path:
    bundle = resolve_run_bundle(campaign_path, run_id, rep_index)
    project = bundle / "dbt_project"
    if project.is_dir():
        return project
    raise FileNotFoundError(f"dbt_project missing in {bundle}")
