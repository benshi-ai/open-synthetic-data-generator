import dataclasses
import logging
import random
import sys
from typing import List, Any, Dict, Optional

from datetime import datetime

from synthetic.constants import LOG_DATETIME_FORMAT
from synthetic.event.constants import NudgeType
from synthetic.sink.http_flush_sink import get_data_with_retries
from synthetic.utils.time_utils import datetime_to_payload_str, datetime_from_payload_str

logger = logging.getLogger(__name__)


def get_dispatched_at_from_nudge_data(nudge_data: Dict[str, Any]) -> datetime:
    dt_str = nudge_data["dispatched_at"] if "dispatched_at" in nudge_data else nudge_data["queued_at"]
    return datetime_from_payload_str(dt_str)


@dataclasses.dataclass
class Nudge:
    nudge_id: int
    subject_id: str
    queued_at: datetime
    nudge_type: NudgeType = NudgeType.PUSH_NOTIFICATION
    action: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, nudge_dict: Dict[str, Any]):
        nudge_id = nudge_dict["nudge_id"]
        subject_id = nudge_dict["subject_id"]
        queued_at = get_dispatched_at_from_nudge_data(nudge_dict)
        assert queued_at is not None
        action = nudge_dict["action"]

        return Nudge(nudge_id=nudge_id, subject_id=subject_id, queued_at=queued_at, action=action)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nudge_id": self.nudge_id,
            "subject_id": self.subject_id,
            "queued_at": self.queued_at.strftime(LOG_DATETIME_FORMAT),
            "action": self.action,
        }


def get_nudges_from_backend(api_url: str, api_key: str, subject_id: str, last_queued_at: datetime) -> List[Nudge]:
    logger.debug("Fetching nudges from backend for %s...", subject_id)
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    params = {"subject_id": subject_id, "queued_at": datetime_to_payload_str(last_queued_at)}
    all_nudge_data = get_data_with_retries(f"{api_url}/nudge/sdk/get", headers=headers, params=params)

    nudges: List[Nudge] = []
    if all_nudge_data is None:
        return nudges

    for nudge_data in all_nudge_data:
        nudge_dispatched_at = datetime_from_payload_str(
            nudge_data["dispatched_at"] if "dispatched_at" in nudge_data else nudge_data["queued_at"]
        )
        assert nudge_dispatched_at is not None
        nudges.append(
            Nudge(
                nudge_id=nudge_data["id"],
                subject_id=subject_id,
                queued_at=nudge_dispatched_at,
                action=nudge_data["action"],
            )
        )

    nudges = sorted(nudges, key=lambda nudge: nudge.queued_at)
    logger.debug("Fetched %s nudges!", len(nudges))
    return nudges


def generate_random_nudge(
    subject_id: str, queued_at: datetime, nudge_type: NudgeType = NudgeType.PUSH_NOTIFICATION
) -> Nudge:
    return Nudge(
        nudge_id=-1 * random.randint(1, sys.maxsize),
        subject_id=subject_id,
        queued_at=queued_at,
        nudge_type=nudge_type,
    )
