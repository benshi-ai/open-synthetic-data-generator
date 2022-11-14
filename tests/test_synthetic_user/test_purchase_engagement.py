import random
from datetime import datetime, timedelta
from uuid import uuid4

import pytest

from synthetic.catalog.cache import CatalogCache
from synthetic.constants import CatalogType, Currency, BlockType
from synthetic.conf import (
    global_conf,
    ProfileConfig,
    EngagementConfig,
    PurchaseBehaviourConfig,
    CatalogConfig,
)
from synthetic.database.schemas import SyntheticUserSchema
from synthetic.event.catalog.blood_catalog import BloodCatalogEvent
from synthetic.event.catalog.drug_catalog import DrugCatalogEvent
from synthetic.event.catalog.medical_equipment_catalog import MedicalEquipmentCatalogEvent
from synthetic.event.catalog.oxygen_catalog import OxygenCatalogEvent
from synthetic.event.constants import EventType
from synthetic.event.log.commerce.cancel_checkout import CancelCheckoutEvent
from synthetic.event.log.commerce.item import get_stock_status_for_timestamp
from synthetic.user.constants import SyntheticUserType
from synthetic.user.factory import store_user_in_db, load_user_from_db
from synthetic.user.purchase_engagement_user import PurchaseEngagementUser
from synthetic.utils.event_utils import prepare_price_for_writing
from synthetic.utils.test_utils import assert_dicts_equal_partial, assert_events_have_correct_schema


@pytest.fixture(autouse=True)
def fixed_seed():
    random.seed(0)
    # pass


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.profiles = {
        "boring_guy": ProfileConfig(
            user_type=SyntheticUserType.PURCHASE_ENGAGEMENT,
            occurrence_probability=1.0,
            session_min_count=1,
            session_max_count=1,
            online_probability=0.5,
            session_engagement=EngagementConfig(
                initial_min=1.0,
                initial_max=1.0,
                change_probability=0.0,
            ),
            purchase_engagement=EngagementConfig(
                initial_min=1.0,
                initial_max=1.0,
                change_min=0.2,
                change_max=0.2,
                change_probability=1.0,
                decay_probability=1.0,
                boost_probability=0.0,
            ),
        )
    }


@pytest.fixture()
def profile_name():
    profile_name = "boring_guy"

    return profile_name


def test_manage_purchase_engagement(db_session, driver_meta, profile_name):
    CatalogCache.warm_up(db_session=db_session)

    start_ts = datetime(2000, 1, 1)
    global_conf.start_ts = start_ts

    user = PurchaseEngagementUser(
        driver_meta.id,
        str(uuid4()),
        profile_data={
            "profile_name": profile_name,
            "registration_timestamp": start_ts.timestamp(),
        },
    )

    for day_index in range(0, 5):
        end_ts = start_ts + timedelta(days=1 + day_index)
        events = user.generate_events(end_ts)
        log_events = sorted(events.log_events, key=lambda event: event.ts)
        assert len(log_events) > 0

        expected_session_engagement = 1.0
        expected_purchase_engagement = 1.0 - (day_index + 1) * 0.2

        assert user.get_manager("session_engagement").get_engagement() == 1.0
        assert (
            pytest.approx(user.get_manager("purchase_engagement").get_engagement(), 10e-5)
            == expected_purchase_engagement
        )
        assert_dicts_equal_partial(
            user.get_profile_data(),
            {
                "managers": {
                    "session_engagement": {
                        "engagement_level": expected_session_engagement,
                        "last_seen_ts": end_ts.timestamp(),
                    },
                    "purchase_engagement": {
                        "engagement_level": pytest.approx(expected_purchase_engagement, 10e-5),
                        "last_seen_ts": end_ts.timestamp(),
                    },
                },
                "profile_name": profile_name,
                "registration_timestamp": start_ts.timestamp(),
            },
        )
        assert user.last_seen_ts == end_ts

    assert user.get_type() == SyntheticUserType.PURCHASE_ENGAGEMENT
    store_user_in_db(db_session, driver_meta.id, user)
    assert db_session.query(SyntheticUserSchema).count() == 1
    loaded_user = load_user_from_db(db_session, driver_meta.id, user.get_platform_uuid())
    assert (
        loaded_user.get_manager("purchase_engagement").get_engagement()
        == user.get_manager("purchase_engagement").get_engagement()
    )


@pytest.mark.parametrize(
    "catalog_type", [CatalogType.BLOOD, CatalogType.DRUG, CatalogType.MEDICAL_EQUIPMENT, CatalogType.OXYGEN]
)
def test_generate_commerce_logs(db_session, driver_meta, profile_name, catalog_type):
    global_conf.catalogs[catalog_type] = CatalogConfig(target_count=10)
    CatalogCache.warm_up(db_session=db_session)

    global_conf.profiles[profile_name].purchase_engagement = EngagementConfig(
        initial_min=1.0,
        initial_max=1.0,
        change_probability=0.0,
    )
    global_conf.profiles[profile_name].behaviour.purchase = PurchaseBehaviourConfig(
        initial_account_balance_min=10000,
        initial_account_balance_max=10000,
        interest_catalog_range_min=0.2,
        interest_catalog_range_max=0.2,
        interest_per_item_min=1.0,
        interest_per_item_max=1.0,
        views_required_per_purchase_min=3,
        views_required_per_purchase_max=3,
        views_per_session_min=1,
        views_per_session_max=1,
        impression_ratio=1.0,
        detail_probability=1.0,
        purchase_count_per_item_min=2,
        purchase_count_per_item_max=2,
        payment_failure_probability_max=0.0,
        checkout_failure_probability_max=0.0,
        checkout_cancellation_probability_min=0.0,
        checkout_cancellation_probability_max=0.0,
        catalog_type_probabilities={catalog_type: 1.0},
        reminder_probability=1.0,
        auto_reminder_type_probability=0.5,
        favorite_probability=1.0,
    )

    start_ts = datetime(2000, 1, 1)
    global_conf.start_ts = start_ts

    user = PurchaseEngagementUser(
        driver_meta.id,
        str(uuid4()),
        profile_data={
            "profile_name": profile_name,
            "registration_timestamp": start_ts.timestamp(),
        },
    )

    interested_items = None
    end_ts = None
    for day_index in range(0, 4):
        end_ts = start_ts + timedelta(days=1 + day_index)
        events = user.generate_events(end_ts)

        log_events = sorted(events.log_events, key=lambda event: event.ts)
        impression_events = [
            event for event in log_events if event.event_type == "item" and event.props["action"] == "impression"
        ]
        view_events = [event for event in log_events if event.event_type == "item" and event.props["action"] == "view"]
        detail_events = [
            event for event in log_events if event.event_type == "item" and event.props["action"] == "detail"
        ]
        favorite_events = [
            event
            for event in log_events
            if event.event_type == "item" and event.props["action"] in ["add_favorite", "remove_favorite"]
        ]
        reminder_events = [
            event
            for event in log_events
            if event.event_type == "item"
            and event.props["action"] in ["add_reminder", "remove_reminder", "remove_reminder_auto"]
        ]

        deferred_payment_events = [event for event in log_events if event.event_type == "deferred_payment"]
        cart_events = [event for event in log_events if event.event_type == "cart"]

        checkout_events = [event for event in log_events if event.event_type == "checkout"]

        cancel_order_events = [event for event in log_events if isinstance(event, CancelCheckoutEvent)]

        assert len(cancel_order_events) == 0
        if day_index == 0:
            assert len(impression_events) > 0
            assert len(view_events) > 0
            assert len(detail_events) > 0
            assert len(favorite_events) > 0
            assert len(reminder_events) > 0

            assert len(deferred_payment_events) == 1
            assert_dicts_equal_partial(
                deferred_payment_events[0].props,
                {'account_balance': 10000.0, 'payment_amount': 10000.0, 'action': 'payment_processed'},
            )
            assert deferred_payment_events[0].block == BlockType.PAYMENT

            assert len(checkout_events) == 0

            interested_items = list(set([event.props["item"]["id"] for event in view_events]))
            assert len(interested_items) > 0

            assert user.account_balance == 10000.0
            user_profile_data = user.get_profile_data()
            assert len(user_profile_data["current_favorites"]) > 0
            assert len(user_profile_data["current_reminders"]) > 0
            assert [interest["remaining_view_count"] for interest in user_profile_data["item_interests"].values()] == [
                2
            ] * len(user_profile_data["item_interests"])

        elif day_index != 2:
            assert len(impression_events) > 0
            assert len(view_events) > 0
            assert len(detail_events) > 0
            assert len(favorite_events) > 0
            assert len(reminder_events) > 0

            user_profile_data = user.get_profile_data()
            assert len(user_profile_data["current_favorites"]) == 0
            assert len(user_profile_data["current_reminders"]) == 0
            if day_index == 1:
                assert [
                    interest["remaining_view_count"] for interest in user_profile_data["item_interests"].values()
                ] == [1] * len(user_profile_data["item_interests"])
            elif day_index == 3:
                assert [
                    interest["remaining_view_count"] for interest in user_profile_data["item_interests"].values()
                ] == [2] * len(user_profile_data["item_interests"])

            assert len(checkout_events) == 0
        else:
            # This is the day we buy stuff
            assert len(impression_events) > 0
            assert len(view_events) > 0
            assert len(detail_events) > 0
            assert len(favorite_events) > 0
            assert len(reminder_events) > 0
            assert len(user.get_profile_data()["current_favorites"]) > 0
            assert len(user.get_profile_data()["current_reminders"]) > 0

            assert len(cart_events) > 0
            assert len(checkout_events) > 0
            # assert len(payment_method_events) > 0

            total_price = 0.0
            items = []
            for item_uuid in sorted(interested_items):
                item_meta = CatalogCache.get_catalog_by_uuid(catalog_type, item_uuid)
                item_price = item_meta["item_price"] if "item_price" in item_meta else item_meta["price"]
                items.append(
                    {
                        "id": item_uuid,
                        "type": catalog_type.value,
                        "price": prepare_price_for_writing(item_price * 2),
                        "quantity": 2,
                        "currency": Currency.USD.value,
                        "stock_status": get_stock_status_for_timestamp(item_uuid, events.log_events[-1].ts).value,
                    }
                )
                total_price += item_price * 2

            assert_dicts_equal_partial(
                checkout_events[0].props,
                {
                    "currency": "USD",
                    "is_successful": True,
                    "items": items,
                    "cart_price": prepare_price_for_writing(total_price),
                },
            )

            milestone_events = [event for event in log_events if event.event_type == "milestone"]
            assert len(milestone_events) > 0
            for milestone_event in milestone_events:
                assert milestone_event.block == BlockType.LOYALTY
            assert len(user.get_profile_data()["milestone_achieved_uuids"]) == len(milestone_events)

            level_events = [event for event in log_events if event.event_type == "level"]
            assert len(level_events) == len(milestone_events)
            for current_level, level_event in enumerate(level_events):
                assert level_event.props["prev_level"] == current_level
                assert level_event.props["new_level"] == current_level + 1
                assert level_event.block == BlockType.LOYALTY

            specific_catalog_events = [
                catalog_event for catalog_event in events.catalog_events if catalog_event.catalog_type == catalog_type
            ]
            assert len(specific_catalog_events) > 0
            for catalog_event in specific_catalog_events:
                if catalog_event.catalog_type == CatalogType.BLOOD:
                    assert isinstance(catalog_event, BloodCatalogEvent)
                elif catalog_event.catalog_type == CatalogType.DRUG:
                    assert isinstance(catalog_event, DrugCatalogEvent)
                elif catalog_event.catalog_type == CatalogType.MEDICAL_EQUIPMENT:
                    assert isinstance(catalog_event, MedicalEquipmentCatalogEvent)
                elif catalog_event.catalog_type == CatalogType.OXYGEN:
                    assert isinstance(catalog_event, OxygenCatalogEvent)
                else:
                    raise ValueError(catalog_event)

            user_profile_data = user.get_profile_data()
            assert [interest["remaining_view_count"] for interest in user_profile_data["item_interests"].values()] == [
                0
            ] * len(user_profile_data["item_interests"])

        assert_events_have_correct_schema(events)

        interests = user.get_profile_data()["item_interests"]
        assert len(interests) > 0

    assert user.get_type() == SyntheticUserType.PURCHASE_ENGAGEMENT

    assert_dicts_equal_partial(
        user.get_profile_data(),
        {
            "item_interests": dict(
                [
                    (item_id, {"catalog_type": catalog_type.value, "interest_ratio": 1.0, "remaining_view_count": 2})
                    for item_id in interests
                ]
            ),
            "managers": {
                "purchase_engagement": {
                    "engagement_level": 1.0,
                    "last_seen_ts": end_ts.timestamp(),
                },
                "session_engagement": {
                    "engagement_level": 1.0,
                    "last_seen_ts": end_ts.timestamp(),
                },
            },
            "profile_name": profile_name,
            "registration_timestamp": start_ts.timestamp(),
        },
    )

    store_user_in_db(db_session, driver_meta.id, user)
    assert db_session.query(SyntheticUserSchema).count() == 1
    loaded_user = load_user_from_db(db_session, driver_meta.id, user.get_platform_uuid())
    assert (
        loaded_user.get_manager("purchase_engagement").get_engagement()
        == user.get_manager("purchase_engagement").get_engagement()
    )

    assert user.get_profile_data()["current_account_balance"] > 0


def test_generate_top_up_payments(db_session, driver_meta, profile_name):
    CatalogCache.warm_up(db_session=db_session)

    global_conf.profiles[profile_name].purchase_engagement = EngagementConfig(
        initial_min=1.0,
        initial_max=1.0,
        change_probability=0.0,
    )
    global_conf.profiles[profile_name].behaviour.purchase = PurchaseBehaviourConfig(
        initial_account_balance_min=0,
        initial_account_balance_max=0,
        top_up_probability=1.0,
        payment_failure_probability_min=0.5,
        payment_failure_probability_max=0.5,
        interest_catalog_range_min=1.0,
        interest_catalog_range_max=1.0,
        interest_per_item_min=1.0,
        interest_per_item_max=1.0,
        views_required_per_purchase_min=1,
        views_required_per_purchase_max=1,
        views_per_session_min=1,
        views_per_session_max=1,
        purchase_count_per_item_min=1,
        purchase_count_per_item_max=1,
    )

    start_ts = datetime(2000, 1, 1)
    global_conf.start_ts = start_ts

    user = PurchaseEngagementUser(
        driver_meta.id,
        str(uuid4()),
        profile_data={
            "profile_name": profile_name,
            "registration_timestamp": start_ts.timestamp(),
        },
    )

    events = user.generate_events(start_ts + timedelta(days=5))
    log_events = sorted(events.log_events, key=lambda event: event.ts)
    deferred_payment_events = [event for event in log_events if event.event_type == "deferred_payment"]
    assert len(deferred_payment_events) >= 3

    assert user.account_balance < 0

    assert_events_have_correct_schema(events)


def test_purchase_engagement_generate_normal_logs(db_session, driver_meta, profile_name):
    global_conf.catalogs[CatalogType.DRUG] = CatalogConfig(target_count=10)
    CatalogCache.warm_up(db_session, driver_meta.id)

    global_conf.profiles[profile_name].purchase_engagement = EngagementConfig(
        initial_min=1.0,
        initial_max=1.0,
        change_probability=0.0,
    )
    global_conf.profiles[profile_name].event_probabilities = {
        EventType.PAGE: 0.5,
        EventType.VIDEO: 0.5,
    }
    global_conf.profiles[profile_name].behaviour.normal_event_probability = 1.0

    start_ts = datetime(2000, 1, 1)
    global_conf.start_ts = start_ts

    user = PurchaseEngagementUser(
        driver_meta.id,
        str(uuid4()),
        profile_data={
            "profile_name": profile_name,
            "registration_timestamp": start_ts.timestamp(),
        },
    )

    all_page_event_count = 0
    all_media_event_count = 0
    for day_index in range(0, 10):
        end_ts = start_ts + timedelta(days=1 + day_index)
        events = user.generate_events(end_ts)
        log_events = events.log_events
        page_event_count = len([event for event in log_events if event.event_type == "page"])
        video_event_count = len([event for event in log_events if event.event_type == "media"])

        all_page_event_count += page_event_count
        all_media_event_count += video_event_count
        assert_events_have_correct_schema(events)

    assert all_page_event_count > 0
    assert all_media_event_count > 0
