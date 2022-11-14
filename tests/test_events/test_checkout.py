import pytest

from datetime import datetime

from synthetic.catalog.cache import CatalogCache
from synthetic.catalog.generator import create_random_catalog_events_for_type
from synthetic.conf import ProfileConfig, global_conf
from synthetic.constants import Currency, CatalogType
from synthetic.event.event_collection import EventCollection
from synthetic.event.log.commerce.checkout import CheckoutEvent
from synthetic.event.log.commerce.constants import ShopItem, ItemType
from synthetic.user.session_engagement_user import SessionEngagementUser
from synthetic.utils.test_utils import assert_dicts_equal_partial, assert_events_have_correct_schema
from synthetic.utils.time_utils import datetime_to_payload_str


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.profiles = {"random_guy": ProfileConfig()}


@pytest.fixture
def registration_ts():
    return datetime(2000, 1, 1, 0, 0, 0)


@pytest.fixture
def checkout_user(driver_meta, registration_ts):
    return SessionEngagementUser.create_random_user(driver_meta.id, registration_ts, profile_name="random_guy")


def test_checkout_event_payload(db_session, checkout_user, registration_ts):
    item_catalogs = create_random_catalog_events_for_type(CatalogType.DRUG, registration_ts)
    for item_catalog in item_catalogs:
        CatalogCache.add_catalog_for_uuid(CatalogType.DRUG, item_catalog.data["uuid"], item_catalog.data)

    online = True
    item_of_interest = item_catalogs[0]
    checkout_event = CheckoutEvent(
        checkout_user,
        registration_ts,
        online=online,
        order_id="order_id",
        cart_id="cart_id",
        total_price=246.0,
        currency=Currency.USD,
        order=[ShopItem(id=item_of_interest.data["uuid"], item_type=ItemType.DRUG, item_price=123.0, quantity=2)],
        is_urgent=False,
    )

    assert_dicts_equal_partial(
        checkout_event.as_payload_dict(),
        {
            'd_id': checkout_user.get_current_device_id(),
            'ts': datetime_to_payload_str(registration_ts),
            'type': 'checkout',
            'u_id': checkout_user.get_platform_uuid(),
            "ol": online,
        },
    )

    assert_dicts_equal_partial(
        checkout_event.as_payload_dict()["props"],
        {
            'currency': 'USD',
            'id': 'order_id',
            'is_successful': True,
            'items': [
                {
                    'id': item_of_interest.data["uuid"],
                    'type': ItemType.DRUG.value,
                    'price': 246.0,
                    'quantity': 2,
                    "currency": Currency.USD.value,
                }
            ],
            'cart_price': 246.0,
            'cart_id': 'cart_id',
        },
    )

    catalog_events = checkout_event.generate_associated_catalog_events()
    assert_events_have_correct_schema(EventCollection(catalog_events=catalog_events))
