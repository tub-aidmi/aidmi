"""Human-readable progress logging to stderr."""
from __future__ import annotations

import sys
from datetime import datetime


def log_message(message: str, *, scope: str | None = None) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if scope:
        print(f"{ts} [{scope}] {message}", file=sys.stderr, flush=True)
    else:
        print(f"{ts} {message}", file=sys.stderr, flush=True)
