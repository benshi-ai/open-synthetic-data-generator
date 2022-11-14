from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict

from synthetic.constants import Currency
from synthetic.utils.event_utils import prepare_price_for_writing


class ItemStockStatus(Enum):
    IN_STOCK = "in_stock"
    LOW_STOCK = "low_stock"
    OUT_OF_STOCK = "out_of_stock"


class ItemType(Enum):
    DRUG = "drug"
    BLOOD = "blood"
    OXYGEN = "oxygen"
    MEDICAL_EQUIPMENT = "medical_equipment"


@dataclass
class ItemObject:
    id: str
    item_type: ItemType

    def get_payload_dict(self) -> Dict:
        return {"id": self.id, "type": self.item_type.value}


@dataclass
class ShopItem:
    id: str
    item_type: ItemType
    item_price: float
    quantity: int = 1
    currency: Currency = Currency.USD
    stock_status: Optional[ItemStockStatus] = None
    promo_id: Optional[str] = None

    def get_payload_dict(self) -> Dict:
        result = {
            "id": self.id,
            "type": self.item_type.value,
            "quantity": self.quantity,
            "price": prepare_price_for_writing(self.item_price * self.quantity),
            "currency": self.currency.value,
        }

        if self.stock_status is not None:
            result["stock_status"] = self.stock_status.value
        if self.promo_id is not None:
            result["promo_id"] = self.promo_id

        return result

    def get_item_object(self) -> ItemObject:
        return ItemObject(id=self.id, item_type=self.item_type)
