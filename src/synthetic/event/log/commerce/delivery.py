from datetime import datetime
from enum import Enum

from synthetic.constants import BlockType
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser


class DeliveryAction(Enum):
    DELIVERED = "delivered"


class DeliveryEvent(LogEvent):
    def __init__(self, user: SyntheticUser, ts: datetime, order_id: str, delivery_id: str, action: DeliveryAction):
        super().__init__(
            user,
            ts,
            True,
            "delivery",
            {
                "order_id": order_id,
                "action": action.value,
                "id": delivery_id,
            },
            block=BlockType.ECOMMERCE,
        )

    def get_schema_path(self) -> str:
        return "events/delivery"
