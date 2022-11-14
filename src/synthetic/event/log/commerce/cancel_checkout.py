from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any

from synthetic.constants import BlockType
from synthetic.event.log.commerce.constants import ItemType, ItemObject
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser


class CancelType(Enum):
    CART = "cart"
    ORDER = "order"


class CancelCheckoutEvent(LogEvent):
    @staticmethod
    def build_props(
        object_id: str, cancel_type: CancelType, items: List[ItemObject], reason: str, meta: Optional[Dict] = None
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "id": object_id,
            "type": cancel_type.value,
            "items": [item.get_payload_dict() for item in items],
            "reason": reason,
        }

        if meta is not None:
            result["meta"] = meta

        return result

    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        object_id: str,
        cancel_type: CancelType,
        items: List[ItemObject],
        reason: str,
        meta: Optional[Dict] = None,
    ):
        super().__init__(
            user,
            ts,
            online,
            "cancel_checkout",
            CancelCheckoutEvent.build_props(object_id, cancel_type, items, reason, meta),
            block=BlockType.ECOMMERCE,
        )

        self._items = items

    def get_schema_path(self) -> str:
        return "events/cancel_checkout"

    def get_associated_item_types(self) -> List[ItemType]:
        return [item.item_type for item in self._items]
