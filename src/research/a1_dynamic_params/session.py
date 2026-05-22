#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


TIMEZONE = "Asia/Shanghai"


def local_datetime(ts: float | None = None, timezone_name: str = TIMEZONE) -> datetime:
    tz = ZoneInfo(timezone_name)
    if ts is None:
        return datetime.now(tz)
    return datetime.fromtimestamp(float(ts), tz=tz)


def get_session_tag(dt: datetime | None = None) -> str:
    current = dt or local_datetime()
    minutes = current.hour * 60 + current.minute
    if minutes < 5 * 60:
        return "US_LATE"
    if minutes < 8 * 60:
        return "ASIA_OFF"
    if minutes < 16 * 60:
        return "ASIA_DAY"
    if minutes < 21 * 60 + 30:
        return "EUROPE_PRE_US"
    return "US_OPEN"


def session_info(ts: float | None = None, timezone_name: str = TIMEZONE) -> dict[str, object]:
    dt = local_datetime(ts, timezone_name)
    return {
        "timezone": timezone_name,
        "local_time": dt.isoformat(),
        "session_tag": get_session_tag(dt),
        "is_weekend": dt.weekday() >= 5,
    }
