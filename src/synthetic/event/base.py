from datetime import datetime
from typing import Optional

from synthetic.utils.time_utils import datetime_to_payload_str


class Event(object):
    def __init__(self, ts: datetime):
        self.ts: datetime = ts

    def get_formatted_ts(self) -> str:
        return datetime_to_payload_str(self.ts)

    def get_schema_path(self) -> Optional[str]:
        raise NotImplementedError()
