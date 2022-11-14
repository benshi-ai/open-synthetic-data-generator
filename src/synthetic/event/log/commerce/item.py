import hashlib
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, List, Tuple

from synthetic.constants import BlockType
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.commerce.constants import ShopItem, ItemType, ItemStockStatus
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.catalog_utils import shop_item_as_catalog_event


class ItemAction(Enum):
    IMPRESSION = "impression"
    VIEW = "view"
    DETAIL = "detail"
    ADD_FAVORITE = "add_favorite"
    REMOVE_FAVORITE = "remove_favorite"
    ADD_REMINDER = "add_reminder"
    REMOVE_REMINDER = "remove_reminder"
    REMOVE_REMINDER_AUTO = "remove_reminder_auto"


def build_item_props(action: ItemAction, shop_item: ShopItem, search_id: Optional[str], usd_rate: float) -> Dict:
    props = {
        "action": action.value,
        "item": shop_item.get_payload_dict(),
        "usd_rate": usd_rate,
    }
    if search_id is not None:
        props["search_id"] = search_id

    return props


def get_stock_status_for_timestamp(item_uuid: str, ts: datetime) -> ItemStockStatus:
    item_uuid_offset = int(hashlib.sha256(item_uuid.encode('utf-8')).hexdigest(), 16)
    day_of_year = ts.timetuple().tm_yday
    stock_status_offset = (item_uuid_offset + day_of_year) % 7
    if stock_status_offset < 4:
        return ItemStockStatus.IN_STOCK
    elif stock_status_offset < 6:
        return ItemStockStatus.LOW_STOCK
    else:
        return ItemStockStatus.OUT_OF_STOCK


class ItemEvent(LogEvent):
    @classmethod
    def build_shop_item_from_meta(
        cls, item_meta, current_ts: datetime, quantity: int = 1, promo_tuple: Optional[Tuple[str, float]] = None
    ):
        item_uuid = item_meta["uuid"]
        item_type = ItemType(item_meta["type"]) if "type" in item_meta else ItemType.DRUG
        item_price = item_meta["item_price"] if "item_price" in item_meta else item_meta["price"]
        promo_id: Optional[str] = None
        if promo_tuple is not None:
            promo_id = promo_tuple[0]
            item_price *= promo_tuple[1]

        return ShopItem(
            id=item_uuid,
            item_type=item_type,
            item_price=item_price,
            quantity=quantity,
            stock_status=get_stock_status_for_timestamp(item_meta["uuid"], current_ts),
            promo_id=promo_id,
        )

    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        shop_item: ShopItem,
        action: ItemAction,
        search_id: Optional[str] = None,
        usd_rate: float = 1.0,
    ):
        super().__init__(
            user,
            ts,
            online,
            "item",
            build_item_props(action=action, shop_item=shop_item, search_id=search_id, usd_rate=usd_rate),
            block=BlockType.ECOMMERCE,
        )

        self._item = shop_item

    def get_schema_path(self) -> str:
        return "events/item"

    def generate_associated_catalog_events(self) -> List[CatalogEvent]:
        return [shop_item_as_catalog_event(self._item, self.ts)]

    def get_associated_item_types(self) -> List[ItemType]:
        return [self._item.item_type]
