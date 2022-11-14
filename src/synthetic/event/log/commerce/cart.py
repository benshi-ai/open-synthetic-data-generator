from datetime import datetime
from enum import Enum
from typing import List

from synthetic.constants import BlockType, Currency
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.commerce.constants import ShopItem, ItemType
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.event_utils import prepare_price_for_writing
from synthetic.utils.catalog_utils import shop_item_as_catalog_event


class CartAction(Enum):
    ADD_ITEM = "add_item"
    REMOVE_ITEM = "remove_item"


class CartEvent(LogEvent):
    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        cart_id: str,
        action: CartAction,
        shop_item: ShopItem,
        cart_price: float,
        currency: Currency = Currency.USD,
        usd_rate: float = 1.0,
    ):
        super().__init__(
            user,
            ts,
            True,
            "cart",
            {
                "id": cart_id,
                "action": action.value,
                "item": shop_item.get_payload_dict(),
                "cart_price": prepare_price_for_writing(cart_price),
                "currency": currency.value,
                "usd_rate": usd_rate,
            },
            block=BlockType.ECOMMERCE,
        )

        self._shop_item = shop_item

    def get_schema_path(self) -> str:
        return "events/cart"

    def generate_associated_catalog_events(self) -> List[CatalogEvent]:
        return [shop_item_as_catalog_event(self._shop_item, self.ts)]

    def get_associated_item_types(self) -> List[ItemType]:
        return [self._shop_item.item_type]
