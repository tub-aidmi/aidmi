from datetime import datetime, timedelta

from aidmi_orchestrator.clock import utc_now


def test_utc_now_is_naive():
    assert utc_now().tzinfo is None


def test_utc_now_tracks_utc():
    now = utc_now()
    assert abs(now - datetime.now()) < timedelta(days=1)
    assert isinstance(now, datetime)
