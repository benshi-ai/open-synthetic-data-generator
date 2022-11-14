from datetime import datetime
from typing import Dict, Optional

from synthetic.constants import BlockType, Currency
from synthetic.event.constants import PaymentType
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.event_utils import prepare_price_for_writing


def build_props(
    order_id: str,
    payment_type: PaymentType,
    payment_amount: float,
    currency: Currency = Currency.USD,
    usd_rate: float = 1.0,
    meta: Optional[Dict] = None,
):
    result = {
        "order_id": order_id,
        "type": payment_type.value,
        "payment_amount": prepare_price_for_writing(payment_amount),
        "currency": currency.value,
        "usd_rate": usd_rate,
    }
    if meta is not None:
        result["meta"] = meta

    return result


class PaymentMethodEvent(LogEvent):
    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        order_id: str,
        payment_type: PaymentType,
        payment_amount: float,
        currency: Currency = Currency.USD,
        usd_rate: float = 1.0,
        meta: Optional[Dict] = None,
    ):
        super().__init__(
            user,
            ts,
            online,
            "payment_method",
            build_props(order_id, payment_type, payment_amount, currency, usd_rate, meta),
            block=BlockType.PAYMENT,
        )

    def get_schema_path(self) -> str:
        return "events/payment_method"
