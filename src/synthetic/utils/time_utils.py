from datetime import datetime
from typing import Optional

from rfc3339 import _timedelta_to_seconds

from synthetic.constants import LOG_DATETIME_FORMAT


def total_difference_seconds(first_ts: datetime, last_ts: datetime) -> int:
    return _timedelta_to_seconds(last_ts - first_ts)


def datetime_to_payload_str(t: Optional[datetime]) -> str:
    if t is None:
        raise ValueError("Cannot format None datetime!")
    return t.strftime(LOG_DATETIME_FORMAT)


def datetime_from_payload_str(t_str: str) -> datetime:
    try:
        return datetime.strptime(t_str, LOG_DATETIME_FORMAT)
    except ValueError:
        return datetime.strptime(t_str, "%Y-%m-%dT%H:%M:%SZ")
