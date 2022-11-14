from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any

from synthetic.constants import BlockType, Currency
from synthetic.event.constants import PaymentType
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.event_utils import prepare_price_for_writing


class DeferredPaymentAction(Enum):
    PAYMENT_PROCESSED = "payment_processed"


def build_props(
    payment_id: str,
    action: DeferredPaymentAction,
    payment_type: PaymentType,
    account_balance: float,
    payment_amount: float,
    currency: Currency = Currency.USD,
    is_successful: bool = True,
    usd_rate: float = 1.0,
    order_id: Optional[str] = None,
) -> Dict[str, Any]:
    props = {
        "id": payment_id,
        "action": action.value,
        "type": payment_type.value,
        "account_balance": prepare_price_for_writing(account_balance),
        "payment_amount": prepare_price_for_writing(payment_amount),
        "currency": currency.value,
        "is_successful": is_successful,
        "usd_rate": usd_rate,
    }

    if order_id is not None:
        props["order_id"] = order_id

    return props


class DeferredPaymentEvent(LogEvent):
    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        payment_id: str,
        action: DeferredPaymentAction,
        payment_type: PaymentType,
        account_balance: float,
        payment_amount: float,
        currency: Currency = Currency.USD,
        is_successful: bool = True,
        usd_rate: float = 1.0,
        order_id: Optional[str] = None,
    ):
        super().__init__(
            user,
            ts,
            True,
            "deferred_payment",
            build_props(
                payment_id,
                action,
                payment_type,
                account_balance,
                payment_amount,
                currency,
                is_successful,
                usd_rate,
                order_id,
            ),
            block=BlockType.PAYMENT,
        )

    def get_schema_path(self) -> str:
        return "events/deferred_payment"
