from datetime import datetime
from enum import Enum
from typing import Optional, Dict

from synthetic.constants import BlockType
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.time_utils import datetime_to_payload_str


class ScheduleDeliveryAction(Enum):
    SCHEDULE = "schedule"
    UPDATE = "update"


def build_props_for_action(
    order_id: str,
    is_urgent: bool,
    action: ScheduleDeliveryAction,
    delivery_ts: datetime,
    meta: Optional[Dict] = None,
):
    result = {
        "order_id": order_id,
        "is_urgent": is_urgent,
        "action": action.value,
        "delivery_ts": datetime_to_payload_str(delivery_ts),
    }
    if meta is not None:
        result["meta"] = meta

    return result


class ScheduleDeliveryEvent(LogEvent):
    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        order_id: str,
        is_urgent: bool,
        action: ScheduleDeliveryAction,
        delivery_ts: datetime,
        meta: Optional[Dict] = None,
    ):
        super().__init__(
            user,
            ts,
            online,
            "schedule_delivery",
            build_props_for_action(order_id, is_urgent, action, delivery_ts, meta),
            block=BlockType.ECOMMERCE,
        )

    def get_schema_path(self) -> str:
        return "events/schedule_delivery"
