import time
from datetime import UTC, datetime, timedelta

from aidmi_orchestrator.clock import utc_now


def test_utc_now_is_naive():
    assert utc_now().tzinfo is None


def test_utc_now_tracks_utc():
    now = utc_now()
    reference = datetime.fromtimestamp(time.time(), UTC).replace(tzinfo=None)
    assert isinstance(now, datetime)
    assert abs(now - reference) < timedelta(seconds=5)
