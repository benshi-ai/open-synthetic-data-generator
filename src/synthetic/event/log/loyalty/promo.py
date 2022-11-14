from datetime import datetime
from enum import Enum
from typing import List, Tuple, Optional, Dict, Any

from synthetic.constants import BlockType
from synthetic.event.log.commerce.constants import ItemType
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser


class PromoType(Enum):
    ADD_TO_CART = "add_to_cart"
    COUPON = "coupon"


class PromoAction(Enum):
    VIEW = "view"
    APPLY = "apply"


def build_props(
    promo_id: str,
    promo_type: PromoType,
    promo_action: PromoAction,
    title: Optional[str],
    items: Optional[List[Tuple[str, ItemType]]] = None,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "id": promo_id,
        "type": promo_type.value,
        "action": promo_action.value,
    }
    if items is not None:
        items_payload = [{"id": item_id, "type": item_type.value} for item_id, item_type in items]
        result["items"] = items_payload
    if title is not None:
        result["title"] = title
    return result


class PromoEvent(LogEvent):
    @staticmethod
    def build_from_catalog(
        user: SyntheticUser, current_ts: datetime, online: bool, promo_catalog: Dict[str, Any], action: PromoAction
    ):
        promo_id = promo_catalog["uuid"]
        promo_ids = promo_catalog['promoted_item_uuids']
        promo_types = [ItemType(type_str) for type_str in promo_catalog['promoted_item_types']]
        promo_type = promo_catalog['type']
        promo_title = promo_catalog['title']

        return PromoEvent(
            user,
            current_ts,
            online,
            promo_id,
            promo_action=action,
            promo_type=PromoType(promo_type),
            title=promo_title,
            items=list(zip(promo_ids, promo_types)),
        )

    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        promo_id: str,
        promo_action: PromoAction,
        promo_type: PromoType,
        title: Optional[str],
        items: Optional[List[Tuple[str, ItemType]]] = None,
    ):
        super().__init__(
            user,
            ts,
            online,
            "promo",
            build_props(promo_id, promo_type, promo_action, title, items),
            block=BlockType.LOYALTY,
        )

        self._items = items

    def get_schema_path(self) -> str:
        return "events/promo"

    def get_associated_item_types(self) -> List[ItemType]:
        return [entry[1] for entry in self._items] if self._items is not None else []
