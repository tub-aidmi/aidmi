"""Git, package, and artifact provenance for campaigns and runs."""

from __future__ import annotations

import hashlib
import subprocess
from datetime import datetime
from importlib.metadata import version
from pathlib import Path

from aidmi_orchestrator.domain import CampaignProvenance, RunProvenance


def _git(*args: str) -> str:
    try:
        proc = subprocess.run(
            ["git", *args],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if proc.returncode != 0:
            return "unknown"
        return proc.stdout.strip() or "unknown"
    except (OSError, subprocess.TimeoutExpired):
        return "unknown"


def git_dirty() -> bool:
    status = _git("status", "--porcelain")
    return status not in ("", "unknown")


def collect_git_provenance() -> dict[str, str | bool]:
    return {
        "git_sha": _git("rev-parse", "HEAD"),
        "git_branch": _git("rev-parse", "--abbrev-ref", "HEAD"),
        "git_dirty": git_dirty(),
    }


def orchestrator_version() -> str:
    try:
        return version("aidmi-orchestrator")
    except Exception:
        return "unknown"


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dbt_project_sha256(dbt_project_path: Path) -> str | None:
    models = dbt_project_path / "models"
    if not models.is_dir():
        return None
    digest = hashlib.sha256()
    for path in sorted(models.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(dbt_project_path).as_posix()
        digest.update(rel.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def make_campaign_provenance(
    *, campaign_id: str, label: str | None = None
) -> CampaignProvenance:
    git = collect_git_provenance()
    return CampaignProvenance(
        id=campaign_id,
        label=label,
        created_at=datetime.utcnow(),
        git_sha=str(git["git_sha"]),
        git_branch=str(git["git_branch"]),
        git_dirty=bool(git["git_dirty"]),
    )


def make_run_provenance(
    *,
    campaign_id: str,
    strategy_spec_path: str | None = None,
    strategy_spec_sha256: str | None = None,
    workspace_run_dir: Path | None = None,
) -> RunProvenance:
    git = collect_git_provenance()
    dbt_hash: str | None = None
    if workspace_run_dir is not None:
        dbt_hash = dbt_project_sha256(workspace_run_dir / "dbt_project")
    return RunProvenance(
        campaign_id=campaign_id,
        git_sha=str(git["git_sha"]),
        git_branch=str(git["git_branch"]),
        git_dirty=bool(git["git_dirty"]),
        orchestrator_version=orchestrator_version(),
        strategy_spec_path=strategy_spec_path,
        strategy_spec_sha256=strategy_spec_sha256,
        dbt_project_sha256=dbt_hash,
        recorded_at=datetime.utcnow(),
    )
