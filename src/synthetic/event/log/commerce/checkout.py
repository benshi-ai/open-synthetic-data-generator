import random
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import List, Optional

from synthetic.constants import Currency, BlockType
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.constants import PaymentType
from synthetic.event.event_collection import EventCollection
from synthetic.event.log.commerce.constants import ShopItem, ItemType, ItemObject
from synthetic.event.log.commerce.schedule_delivery import ScheduleDeliveryEvent, ScheduleDeliveryAction
from synthetic.event.log.log_base import LogEvent
from synthetic.event.log.payment.payment_method import PaymentMethodEvent
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.event_utils import prepare_price_for_writing
from synthetic.utils.catalog_utils import shop_item_as_catalog_event
from synthetic.utils.random import get_random_delivery_delay_seconds


class ListType(Enum):
    CART = "cart"
    FAVORITE = "favorite"
    REMINDER = "reminder"
    ORDER = "order"


class ListAction(Enum):
    ADD_ITEM = "add_item"
    REMOVE_ITEM = "remove_item"
    EDIT_ITEM = "edit_item"
    VIEW = "view"
    DISCARD = "discard"


class CheckoutEvent(LogEvent):
    @staticmethod
    def build_props(
        order_id: str,
        total_price: float,
        order: List[ShopItem],
        currency: Currency = Currency.USD,
        is_successful: bool = True,
        usd_rate: float = 1.0,
        cart_id: Optional[str] = None,
    ):
        result = {
            "id": order_id,
            "items": [order_item.get_payload_dict() for order_item in order],
            "cart_price": prepare_price_for_writing(total_price),
            "currency": currency.value,
            "is_successful": is_successful,
            "usd_rate": usd_rate,
        }

        if cart_id is not None:
            result["cart_id"] = cart_id

        return result

    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        order_id: str,
        total_price: float,
        order: List[ShopItem],
        is_urgent: bool,
        currency: Currency = Currency.USD,
        is_successful: bool = True,
        usd_rate: float = 1.0,
        cart_id: Optional[str] = None,
        will_be_cancelled: bool = False,
        update_event_count: int = 0,
    ):
        super().__init__(
            user,
            ts,
            online,
            "checkout",
            CheckoutEvent.build_props(
                order_id, total_price, order, currency, is_successful, cart_id=cart_id, usd_rate=usd_rate
            ),
            block=BlockType.ECOMMERCE,
        )

        self._order = order
        self._is_urgent = is_urgent
        self._will_be_cancelled = will_be_cancelled
        self._total_order_price = total_price
        self._update_event_count = update_event_count

    def get_schema_path(self) -> str:
        return "events/checkout"

    def update_driver_after_flush(self, driver: "Driver"):  # type: ignore
        if not self.props.get("is_successful"):
            return

        order_id: str = str(self.props.get("id"))
        # Schedule this order for delivery
        delivery_id = str(uuid.uuid4())
        ideal_delivery_delay_seconds = get_random_delivery_delay_seconds(
            self.user.get_profile_conf().behaviour.schedule.delivery_delay_max_days, self._is_urgent
        )
        ideal_delivery_ts = self.ts + timedelta(seconds=ideal_delivery_delay_seconds)
        delivery_ts: datetime = ideal_delivery_ts

        if not self._is_urgent:
            earliest_delivery_hour = 8
            latest_delivery_hour = 20
            if delivery_ts.hour >= latest_delivery_hour:
                delivery_ts += timedelta(days=1)

            delivery_ts = datetime(delivery_ts.year, delivery_ts.month, delivery_ts.day, earliest_delivery_hour)

            # Weekdays only
            while delivery_ts.weekday() > 4:
                delivery_ts += timedelta(days=1)

            delivery_offset_seconds = (latest_delivery_hour - earliest_delivery_hour) * 3600 * random.random()
            delivery_ts += timedelta(seconds=delivery_offset_seconds)

        assert delivery_ts > self.ts + timedelta(seconds=1000)

        current_ts: datetime = self.ts + timedelta(seconds=random.randint(5, 30))
        checkout_derived_events: List[LogEvent] = []

        payment_method = random.sample(list(PaymentType), k=1)[0]
        payment = PaymentMethodEvent(
            user=self.user,
            ts=current_ts,
            online=self.online,
            order_id=order_id,
            payment_type=payment_method,
            payment_amount=self._total_order_price,
        )
        checkout_derived_events.extend([payment])
        current_ts = current_ts + timedelta(seconds=random.randint(5, 30))

        total_delivery_wait_seconds = (delivery_ts - current_ts).seconds
        events_end_ts = delivery_ts if not self._will_be_cancelled else current_ts + timedelta()
        if self._will_be_cancelled:
            events_end_ts = current_ts + timedelta(seconds=round(total_delivery_wait_seconds * random.random()))
        if self._update_event_count > 0:
            total_end_wait_seconds = (events_end_ts - current_ts).seconds
            update_ratios: List[float] = sorted([random.random() for _ in range(0, self._update_event_count)])
            update_ts_list: List[datetime] = [
                current_ts + timedelta(seconds=total_end_wait_seconds * update_ratio) for update_ratio in update_ratios
            ]
            checkout_derived_events.extend(
                [
                    ScheduleDeliveryEvent(
                        self.user,
                        update_ts,
                        online=self.online,
                        order_id=order_id,
                        is_urgent=self._is_urgent,
                        action=ScheduleDeliveryAction.UPDATE,
                        delivery_ts=delivery_ts,
                    )
                    for update_ts in update_ts_list
                ]
            )

        checkout_derived_events.append(
            ScheduleDeliveryEvent(
                self.user,
                current_ts,
                online=self.online,
                order_id=order_id,
                is_urgent=self._is_urgent,
                action=ScheduleDeliveryAction.SCHEDULE,
                delivery_ts=delivery_ts,
            )
        )

        driver.schedule_detached_events(EventCollection(log_events=checkout_derived_events))

        if self._will_be_cancelled:
            items: List[ItemObject] = [item.get_item_object() for item in self._order]
            driver.schedule_order_cancellation(
                events_end_ts,
                self.online,
                self.user,
                order_id,
                items,
                total_order_price=self._total_order_price,
                reason="Unknown",
            )
        else:
            driver.schedule_delivery(
                user=self.user,
                order_id=order_id,
                order_item_ids=[item.id for item in self._order],
                order_item_types=[item.item_type for item in self._order],
                delivery_id=delivery_id,
                delivery_ts=delivery_ts,
            )

    def generate_associated_catalog_events(self) -> List[CatalogEvent]:
        return [shop_item_as_catalog_event(item, self.ts) for item in self._order]

    def get_associated_item_types(self) -> List[ItemType]:
        return [item.item_type for item in self._order]
