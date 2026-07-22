"""Single source of wall-clock time for recorded artifacts."""

from __future__ import annotations

from datetime import UTC, datetime


def utc_now() -> datetime:
    """Naive UTC timestamp, matching what datetime.utcnow() used to return."""
    return datetime.now(UTC).replace(tzinfo=None)
