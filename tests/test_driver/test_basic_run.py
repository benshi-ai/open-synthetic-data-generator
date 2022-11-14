import json
import os
import csv
import shutil
from typing import List, Optional

import pytest
import random

from datetime import datetime, timedelta, date

from mock import mock

from synthetic.catalog.cache import CatalogCache
from synthetic.constants import CatalogType
from synthetic.conf import CatalogConfig, EngagementConfig, PopulationConfig, ProfileConfig, global_conf, NudgeConfig
from synthetic.database.schemas import SyntheticUserSchema, CatalogEntrySchema
from synthetic.driver.driver import Driver
from synthetic.event.constants import EventType, NudgeResponseAction
from synthetic.event.log.commerce.cancel_checkout import CancelCheckoutEvent, CancelType
from synthetic.event.log.commerce.checkout import CheckoutEvent
from synthetic.event.log.commerce.constants import ItemType
from synthetic.event.log.commerce.delivery import DeliveryEvent, DeliveryAction
from synthetic.event.log.commerce.schedule_delivery import ScheduleDeliveryEvent, ScheduleDeliveryAction
from synthetic.event.log.general.rate import RateEvent
from synthetic.event.log.loyalty.promo import PromoEvent, PromoAction
from synthetic.event.log.nudge.nudge_response import NudgeResponseEvent
from synthetic.event.log.payment.payment_method import PaymentMethodEvent
from synthetic.event.meta.receive_nudge import ReceiveNudges
from synthetic.user.constants import SyntheticUserType
from synthetic.utils.database import create_db_session
from synthetic.user.factory import load_users_from_db
from synthetic.utils.nudge_utils import Nudge
from synthetic.utils.test_utils import assert_dicts_equal_partial, assert_events_have_correct_schema
from synthetic.utils.validation import validate_generated_data


@pytest.fixture(autouse=True)
def fixed_seed():
    random.seed(0)


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.start_ts = datetime(2000, 1, 1, 0, 0, 0)

    global_conf.population = PopulationConfig(initial_count=1, target_max_count=1, target_min_count=1)

    global_conf.profiles = {
        "boring_guy": ProfileConfig(
            occurrence_probability=1.0,
            session_min_count=1,
            session_max_count=1,
            online_probability=0.5,
            session_engagement=EngagementConfig(change_probability=0.0, initial_min=1.0, initial_max=1.0),
            event_probabilities={EventType.PAGE: 1.0},
            nudges=NudgeConfig(
                engagement_effect=EngagementConfig(change_probability=1.0, boost_probability=1.0, decay_probability=0.0)
            ),
        )
    }


@mock.patch("synthetic.sink.memory_flush_sink.MemoryFlushSink.flush_log_events")
@mock.patch("synthetic.user.synthetic_user.get_nudges_from_backend")
@mock.patch("synthetic.utils.current_time_utils.datetime")
def test_nudge_integration(m_datetime, m_get_nudges, m_memory_flush_log_events, fixed_seed):
    m_datetime.now.return_value = datetime.now()
    first_end_ts = datetime(2000, 1, 3, 0, 0, 0)

    global_conf.use_nudges = True
    global_conf.artificial_nudge_min_registration_delay_days = 0

    profile_conf = global_conf.profiles["boring_guy"]
    profile_conf.nudges.checks_per_day_min = 1
    profile_conf.nudges.checks_per_day_max = 1

    global_conf.end_ts = first_end_ts

    driver = Driver(clear_cache_after_flush=False)
    driver.run()

    all_users = driver.get_active_users()
    assert len(all_users) == 1

    # We shouldn't have called the backend for nudges in the past!
    m_get_nudges.assert_not_called()

    receive_nudge_events = [event for event in driver.get_cached_meta_events() if isinstance(event, ReceiveNudges)]
    assert len(receive_nudge_events) == 2
    events = driver.get_cached_events()
    assert_events_have_correct_schema(events)
    driver.clear_cache()

    another_user_nudge = Nudge(nudge_id=1, subject_id="another_user", queued_at=first_end_ts + timedelta(minutes=10))
    m_get_nudges.return_value = [another_user_nudge]

    assert driver.last_seen_ts == global_conf.end_ts

    # Online mode
    profile_conf.nudges.checks_per_day_min = 1000
    profile_conf.nudges.checks_per_day_max = 1000

    driver.set_clear_cache_after_flush(True)
    global_conf.end_ts = first_end_ts + timedelta(minutes=20)
    m_datetime.now.return_value = global_conf.end_ts
    driver.run()
    user = driver.get_active_users()[0]

    assert m_get_nudges.call_count > 10
    m_get_nudges.assert_called_with(
        global_conf.api_url, 'unknown', user.get_platform_uuid(), last_queued_at=datetime(2000, 1, 2, 20, 20)
    )

    user_nudge_id = 2
    user_nudge = Nudge(
        nudge_id=user_nudge_id, subject_id=user.get_platform_uuid(), queued_at=first_end_ts + timedelta(minutes=30)
    )

    returned_nudges = [[]] * 100
    returned_nudges[0] = [user_nudge]
    m_get_nudges.side_effect = returned_nudges

    global_conf.end_ts = first_end_ts + timedelta(minutes=50)
    m_datetime.now.return_value = global_conf.end_ts

    driver.set_clear_cache_after_flush(False)
    driver.run()

    user = driver.get_active_users()[0]

    nudge_logs = set()
    for call_args in m_memory_flush_log_events.call_args_list:
        nudge_logs.update([log for log in call_args[0][0] if isinstance(log, NudgeResponseEvent)])

    assert len(nudge_logs) > 18
    # "Real" nudge
    assert len([nudge_log for nudge_log in nudge_logs if nudge_log.props["nudge_id"] == user_nudge_id]) == 1
    # "Fake" nudges
    assert len([nudge_log for nudge_log in nudge_logs if nudge_log.props["nudge_id"] < 0]) == len(nudge_logs) - 1

    assert user.get_last_received_nudge_ts() > first_end_ts


@mock.patch("synthetic.sink.memory_flush_sink.MemoryFlushSink.flush_log_events")
@mock.patch("synthetic.user.synthetic_user.get_nudges_from_backend")
@mock.patch("synthetic.utils.current_time_utils.datetime")
def test_nudge_response_volume_difference(m_datetime, m_get_nudges, m_memory_flush_log_events, fixed_seed):
    m_datetime.now.return_value = datetime.now()
    first_end_ts = datetime(2000, 1, 3, 0, 0, 0)

    global_conf.population = PopulationConfig(initial_count=4, target_max_count=4, target_min_count=4)
    global_conf.use_nudges = True
    global_conf.artificial_nudge_min_registration_delay_days = 0

    profile_conf = global_conf.profiles["boring_guy"]
    profile_conf.nudges.checks_per_day_min = 1
    profile_conf.nudges.checks_per_day_max = 1
    profile_conf.nudges.bonus_session_count = 10
    profile_conf.nudges.bonus_session_days = 10

    global_conf.end_ts = first_end_ts

    driver = Driver(clear_cache_after_flush=False)
    driver.run()

    all_users = driver.get_active_users()
    nudged_user_id = all_users[0].get_platform_uuid()
    assert len(all_users) == 4

    # Online mode
    profile_conf.nudges.checks_per_day_min = 24
    profile_conf.nudges.checks_per_day_max = 24

    for day_offset in range(0, 5):
        global_conf.end_ts = first_end_ts + timedelta(days=day_offset)
        m_datetime.now.return_value = global_conf.end_ts

        user_nudge = Nudge(
            nudge_id=day_offset, subject_id=nudged_user_id, queued_at=global_conf.end_ts - timedelta(hours=12)
        )
        m_get_nudges.return_value = [user_nudge]

        driver.run()

    events = driver.get_cached_events()
    nudge_response_events = [event for event in events.log_events if isinstance(event, NudgeResponseEvent)]
    assert len(nudge_response_events) > 5

    user_logs = {}
    for event in events.log_events:
        user_id = event.user.get_platform_uuid()
        if user_id not in user_logs:
            user_logs[user_id] = []

        user_logs[user_id].append(event)

    user_log_counts = [(user_id, len(user_logs[user_id])) for user_id in user_logs]
    user_log_counts = sorted(user_log_counts, reverse=True, key=lambda entry: entry[1])

    assert user_log_counts[0][0] == nudged_user_id
    assert user_log_counts[0][1] > round(user_log_counts[1][1] * 1.5)


def test_resumed_events_memory():
    first_end_ts = datetime(2000, 1, 3, 0, 0, 0)
    global_conf.end_ts = first_end_ts

    driver = Driver(clear_cache_after_flush=False)
    driver.run()

    # Check the DB user
    db_session = create_db_session()
    all_users = db_session.query(SyntheticUserSchema).all()
    assert len(all_users) == 1
    user = all_users[0]
    assert user.last_seen_ts == first_end_ts

    first_events = driver.get_cached_log_events()
    assert len(first_events) > 0

    second_end_ts = datetime(2000, 1, 5, 0, 0, 0)
    global_conf.end_ts = second_end_ts
    resumed_driver = Driver(clear_cache_after_flush=False)
    resumed_driver.run()
    second_events = resumed_driver.get_cached_log_events()
    assert len(second_events) > 0

    latest_first_event = max(first_events, key=lambda event: event.ts)
    assert global_conf.start_ts < latest_first_event.ts < first_end_ts

    # Check that we resumed
    earliest_second_event = min(second_events, key=lambda event: event.ts)
    assert first_end_ts < earliest_second_event.ts < second_end_ts

    for user in resumed_driver.get_active_users():
        assert len(user.get_profile_data()["active_dates"]) <= 3


def test_normal_resurrection_memory():
    global_conf.profiles["boring_guy"].session_engagement = EngagementConfig(
        initial_min=1.0,
        initial_max=1.0,
        change_probability=1.0,
        boost_probability=0.0,
        decay_probability=1.0,
        change_min=1.0,
        change_max=1.0,
    )
    global_conf.population.resurrection_probability = 1.0

    for day_index in range(1, 6):
        global_conf.end_ts = global_conf.start_ts + timedelta(days=day_index) - timedelta(hours=1)

        driver = Driver(clear_cache_after_flush=False)
        driver.run()

        assert len(driver.get_active_users()) <= 2

    # Check the user status - they should all have been resurrected and seen something up until the last day
    db_session = create_db_session()
    all_users = db_session.query(SyntheticUserSchema).all()
    for user in all_users:
        assert user.last_seen_ts >= global_conf.start_ts + timedelta(days=4)


def nudge_everybody(api_url: str, api_key: str, subject_id: str, last_queued_at: datetime) -> List[Nudge]:
    # Whomsoever asketh, shall receiveth
    return [Nudge(nudge_id=1, subject_id=subject_id, queued_at=last_queued_at)]


@pytest.mark.parametrize("online", [True, False])
@mock.patch("synthetic.driver.driver.get_current_time")
@mock.patch("synthetic.driver.driver.get_nudges_from_backend", wraps=nudge_everybody)
def test_nudge_resurrection(m_get_nudges, m_get_current_time, online):
    global_conf.profiles["boring_guy"].session_engagement = EngagementConfig(
        initial_min=1.0,
        initial_max=1.0,
        change_probability=1.0,
        boost_probability=0.0,
        decay_probability=1.0,
        change_min=1.0,
        change_max=1.0,
    )

    global_conf.profiles["boring_guy"].configure_early_start()
    global_conf.use_nudges = True
    global_conf.artificial_nudge_min_registration_delay_days = 0
    global_conf.population.inactive_nudge_check_ratio_per_hour = 1.0

    if online:
        m_get_current_time.return_value = global_conf.start_ts + timedelta(days=5) - timedelta(hours=1)
    else:
        m_get_current_time.return_value = datetime(2022, 2, 2)

    for day_index in range(1, 6):
        global_conf.end_ts = global_conf.start_ts + timedelta(days=day_index) - timedelta(hours=1)

        driver = Driver(clear_cache_after_flush=True)
        driver.run()

        events = driver.get_and_clear_memory_sink_events()

        assert_events_have_correct_schema(events)

        nudge_response_events = [event for event in events.log_events if isinstance(event, NudgeResponseEvent)]
        assert len(nudge_response_events) > 0, "No nudge responses on day %s" % (day_index,)

        assert len(driver.get_active_users()) <= 5

    # Check the user status - they should all have been resurrected and seen something up until the last day
    db_session = create_db_session()
    all_users = db_session.query(SyntheticUserSchema).all()
    assert len(all_users) > 3

    online_count = 0
    for user in all_users:
        if user.last_seen_ts >= global_conf.start_ts + timedelta(days=4):
            online_count += 1

    if online:
        assert online_count >= len(all_users) - 3
    else:
        assert online_count < len(all_users)


@mock.patch("synthetic.driver.driver.get_current_time")
@mock.patch("synthetic.driver.driver.get_nudges_from_backend", wraps=nudge_everybody)
def test_very_delayed_nudge_resurrection(m_get_nudges, m_get_current_time):
    global_conf.profiles["boring_guy"].session_engagement = EngagementConfig(
        initial_min=1.0,
        initial_max=1.0,
        change_probability=0.0,
    )

    global_conf.use_nudges = True
    global_conf.profiles["boring_guy"].nudges.response_probabilities = {
        NudgeResponseAction.OPEN: 1.0,
        NudgeResponseAction.DISCARD: 0.0,
        NudgeResponseAction.BLOCK: 0.0,
    }
    global_conf.profiles["boring_guy"].nudges.max_bonus_session_count = 1

    global_conf.artificial_nudge_min_registration_delay_days = 0
    global_conf.population.inactive_nudge_check_ratio_per_hour = 1.0

    m_get_current_time.return_value = global_conf.start_ts + timedelta(days=720)

    # First we get a user to join
    global_conf.end_ts = global_conf.start_ts + timedelta(hours=1)
    driver = Driver(clear_cache_after_flush=False)
    driver.run()
    assert len(driver.get_active_users()) == 1
    user = driver.get_active_users()[0]
    user_uuid = user.get_platform_uuid()
    assert len(driver.get_inactive_users()) == 0

    global_conf.population.target_min_count = 0
    global_conf.population.target_max_count = 0

    # Now we get the user to churn
    global_conf.end_ts = global_conf.start_ts + timedelta(days=1)
    driver = Driver(clear_cache_after_flush=False)
    driver.run()
    driver.force_user_to_churn(user_uuid)
    assert len(driver.get_active_users()) == 0
    assert len(driver.get_inactive_users()) == 1

    user = driver.get_inactive_user_with_uuid(user_uuid)
    last_seen_ts = user.last_seen_ts

    # Now a lot of time passes
    global_conf.end_ts = global_conf.start_ts + timedelta(days=365)
    driver = Driver(clear_cache_after_flush=False)
    driver.run()
    assert len(driver.get_active_users()) == 0
    assert len(driver.get_inactive_users()) == 1
    assert len(driver.get_cached_log_events()) == 0

    # User should not have come back at any point
    user = driver.get_inactive_user_with_uuid(user_uuid)
    assert user.last_seen_ts == last_seen_ts
    # User should also not have retrieved any nudges
    assert m_get_nudges.call_count == 0

    # Now, we give user a nudge
    global_conf.end_ts = global_conf.start_ts + timedelta(days=366)
    m_get_current_time.return_value = global_conf.end_ts
    driver = Driver(clear_cache_after_flush=False)
    driver.run()

    assert driver.last_seen_ts == global_conf.end_ts
    assert m_get_nudges.call_count > 0
    user = driver.get_active_user_with_uuid(user_uuid)
    assert user.last_seen_ts > last_seen_ts
    assert len(driver.get_cached_log_events()) < 100


def test_catalog_events_memory():
    global_conf.catalogs = {
        CatalogType.MEDIA_VIDEO: CatalogConfig(target_count=10),
        CatalogType.EXAM: CatalogConfig(properties={"question_count_min": 5, "question_count_max": 5}),
    }
    global_conf.population = PopulationConfig(initial_count=5, target_max_count=5, target_min_count=5)
    global_conf.profiles["boring_guy"].event_probabilities = {EventType.VIDEO: 1.0}

    first_end_ts = datetime(2000, 1, 3, 0, 0, 0)
    global_conf.end_ts = first_end_ts

    driver = Driver(clear_cache_after_flush=False)
    driver.run()

    catalog_events = driver.get_cached_catalog_events()
    assert sorted(list([catalog_type.value for catalog_type in CatalogCache.cached_catalog])) == [
        'app',
        'blood',
        'drug',
        'elearning_shop_item',
        'exam',
        'media_audio',
        'media_image',
        'media_video',
        'medical_equipment',
        'milestone',
        'module',
        'order',
        'oxygen',
        'page',
        'promo',
        'question',
    ]

    assert len(CatalogCache.cached_catalog[CatalogType.APP]) == 0
    assert len(CatalogCache.cached_catalog[CatalogType.BLOOD]) == 30
    assert len(CatalogCache.cached_catalog[CatalogType.DRUG]) == 30
    assert len(CatalogCache.cached_catalog[CatalogType.ELEARNING_SHOP_ITEM]) == 30
    assert len(CatalogCache.cached_catalog[CatalogType.EXAM]) == 30
    assert len(CatalogCache.cached_catalog[CatalogType.MEDIA_AUDIO]) == 30
    assert len(CatalogCache.cached_catalog[CatalogType.MEDIA_IMAGE]) == 30
    assert len(CatalogCache.cached_catalog[CatalogType.MEDIA_VIDEO]) == 10
    assert len(CatalogCache.cached_catalog[CatalogType.MEDICAL_EQUIPMENT]) == 30
    assert len(CatalogCache.cached_catalog[CatalogType.MILESTONE]) == 30
    assert len(CatalogCache.cached_catalog[CatalogType.MODULE]) == 30
    assert len(CatalogCache.cached_catalog[CatalogType.ORDER]) == 0
    assert len(CatalogCache.cached_catalog[CatalogType.OXYGEN]) == 30
    assert len(CatalogCache.cached_catalog[CatalogType.PAGE]) == 30
    assert len(CatalogCache.cached_catalog[CatalogType.PROMO]) == 0
    assert len(CatalogCache.cached_catalog[CatalogType.QUESTION]) == 150

    for catalog_event in catalog_events:
        assert json.dumps(catalog_event.data) is not None


def test_validation_memory():
    global_conf.catalogs = {CatalogType.MEDIA_VIDEO: CatalogConfig(target_count=10)}
    global_conf.population = PopulationConfig(initial_count=10, target_min_count=5, target_max_count=20, volatility=0.1)

    boring_guy_profile = global_conf.profiles["boring_guy"]
    boring_guy_profile.session_engagement_count_factor = 1.0
    boring_guy_profile.session_engagement_duration_factor = 1.0
    boring_guy_profile.event_probabilities = {EventType.VIDEO: 1.0}
    boring_guy_profile.session_engagement = EngagementConfig(change_probability=0.5, initial_min=0.0, initial_max=1.0)

    first_end_ts = datetime(2000, 1, 2, 0, 0, 0)
    global_conf.end_ts = first_end_ts

    driver = Driver(clear_cache_after_flush=False)
    driver.run()

    events = driver.get_and_clear_memory_sink_events()

    log_data, catalog_data = driver.get_cached_data_as_generated()
    validate_generated_data(log_data, catalog_data, plot=False)
    assert_events_have_correct_schema(events)


def test_variable_management():
    global_conf.catalogs = {CatalogType.MEDIA_VIDEO: CatalogConfig(target_count=10)}
    global_conf.population = PopulationConfig(initial_count=10, target_min_count=5, target_max_count=20, volatility=0.1)

    boring_guy_profile = global_conf.profiles["boring_guy"]
    boring_guy_profile.session_engagement_count_factor = 1.0
    boring_guy_profile.session_engagement_duration_factor = 1.0
    boring_guy_profile.event_probabilities = {EventType.VIDEO: 1.0}
    boring_guy_profile.session_engagement = EngagementConfig(
        change_probability=1.0,
        initial_min=1.0,
        initial_max=1.0,
        boost_probability=0.0,
        decay_probability=1.0,
        change_min=0.2,
        change_max=0.2,
    )

    global_conf.end_ts = datetime(2000, 1, 1, 0, 1, 0)
    driver = Driver(clear_cache_after_flush=False)
    driver.run()

    # Variables should not update immediately
    for user in driver.get_active_users():
        user_data = user.get_profile_data()

        assert user_data["managers"]["session_engagement"] == {
            'engagement_level': 1.0,
            'last_seen_ts': float(user_data["registration_timestamp"]),
        }

    global_conf.end_ts = datetime(2000, 1, 2, 0, 0, 0)
    driver = Driver(clear_cache_after_flush=False)
    driver.run()

    # On the next day, variables are updated
    for user in driver.get_active_users():
        user_data = user.get_profile_data()

        assert user_data["managers"]["session_engagement"] == {
            'engagement_level': 0.8,
            'last_seen_ts': global_conf.end_ts.timestamp(),
        }


def test_events_csv(temp_dir):
    first_end_ts = datetime(2000, 1, 3, 0, 0, 0)
    global_conf.end_ts = first_end_ts

    output_dirname = os.path.join(temp_dir, "csv")
    if os.path.exists(output_dirname):
        shutil.rmtree(output_dirname)

    output_log_csv_filename = os.path.join(output_dirname, "test.csv")
    global_conf.log_events_filename = output_log_csv_filename
    output_catalog_csv_filename = os.path.join(output_dirname, "test_catalog.csv")
    global_conf.catalog_events_filename = output_catalog_csv_filename

    driver = Driver(sink_types=["csv"])
    driver.run()

    # Driver should not have any cached events as they should all have been written to CSV
    assert len(driver.get_cached_log_events()) == 0

    db_session = create_db_session()
    users = load_users_from_db(db_session, driver_meta_id=driver.driver_meta_id)
    assert len(users) == 1
    user = users[0]

    assert os.path.exists(output_log_csv_filename)
    with open(output_log_csv_filename, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        rows = list(csv_reader)
        assert sorted(rows[0]) == sorted(['u_id', 'd_id', 'os', 'ol', 'ts', 'type', 'block', 'ip', 'up', 'dn', 'props'])
        first_row_dict = dict(zip(rows[0], rows[1]))
        assert_dicts_equal_partial(
            first_row_dict,
            {
                'u_id': user.get_platform_uuid(),
                'd_id': user.get_current_device_id(),
                'os': 'android',
                'ip': '0.0.0.0',
                'ol': 'True',
                'ts': '2000-01-01T00:00:00.000000Z',
                'type': 'identify',
                'block': 'core',
            },
        )

        assert len(first_row_dict["props"]) > 0

    assert os.path.exists(output_catalog_csv_filename)
    with open(output_catalog_csv_filename, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        rows = list(csv_reader)
        assert rows[0] == ['ts', 'subject_type', 'data']
        first_row_dict = dict(zip(rows[0], rows[1]))
        assert_dicts_equal_partial(
            first_row_dict,
            {
                "subject_type": "media",
            },
        )
        assert len(first_row_dict['data']) > 0


@pytest.mark.parametrize("checkout_fails", [True, False])
def test_order_delivery(checkout_fails):
    global_conf.start_ts = datetime(2001, 1, 1)
    global_conf.profiles["boring_guy"].session_engagement = EngagementConfig(
        initial_min=1.0,
        initial_max=1.0,
        change_probability=1.0,
        boost_probability=0.0,
        decay_probability=1.0,
        change_min=1.0,
        change_max=1.0,
    )

    global_conf.profiles["boring_guy"].user_type = SyntheticUserType.PURCHASE_ENGAGEMENT
    global_conf.profiles["boring_guy"].behaviour.schedule.delivery_delay_max_days = 0
    global_conf.profiles["boring_guy"].configure_early_start()
    global_conf.rating_probability = 1.0

    failure_probability = 1.0 if checkout_fails else 0.0
    global_conf.profiles["boring_guy"].behaviour.purchase.checkout_failure_probability_min = failure_probability
    global_conf.profiles["boring_guy"].behaviour.purchase.checkout_failure_probability_max = failure_probability
    global_conf.profiles["boring_guy"].behaviour.purchase.checkout_urgent_probability_min = 0.0
    global_conf.profiles["boring_guy"].behaviour.purchase.checkout_urgent_probability_max = 0.0

    assert global_conf.profiles["boring_guy"].behaviour.purchase.interest_catalog_range_min == 0.1
    assert global_conf.start_ts.weekday() == 0, "Need to start this experiment on a Monday!"

    for day_index in range(1, 3):
        global_conf.end_ts = global_conf.start_ts + timedelta(days=day_index)

        driver = Driver(clear_cache_after_flush=True)
        driver.run()

        if day_index == 1:
            events = driver.get_and_clear_memory_sink_events()
            assert_events_have_correct_schema(events)

            scheduled_deliveries = driver.get_scheduled_order_deliveries()
            active_users = driver.get_active_users()
            assert len(active_users) == 1

            if checkout_fails:
                assert len(scheduled_deliveries) == 0
            else:
                assert len(scheduled_deliveries) > 0
                assert all(
                    [
                        ts <= global_conf.start_ts + timedelta(days=2)
                        for ts in [
                            datetime.fromtimestamp(delivery['delivery_timestamp']) for delivery in scheduled_deliveries
                        ]
                    ]
                )
        else:
            events = driver.get_and_clear_memory_sink_events()
            assert_events_have_correct_schema(events)

            delivery_rate_events = [event for event in events.log_events if isinstance(event, DeliveryEvent)]
            if checkout_fails:
                assert len(delivery_rate_events) == 0
            else:
                assert len(delivery_rate_events) > 0
                for delivery_event in delivery_rate_events:
                    assert delivery_event.props["action"] == DeliveryAction.DELIVERED.value

                    assert 0 <= delivery_event.ts.weekday() <= 4
                    assert 8 <= delivery_event.ts.hour <= 21

                rate_events = [event for event in events.log_events if isinstance(event, RateEvent)]
                app_rate_events = [
                    rate_event for rate_event in rate_events if rate_event.props["type"] == CatalogType.APP.value
                ]
                delivery_rate_events = [
                    rate_event for rate_event in rate_events if rate_event.props["type"] == CatalogType.ORDER.value
                ]
                item_rate_events = [
                    rate_event
                    for rate_event in rate_events
                    if rate_event.props["type"] not in [CatalogType.APP.value, CatalogType.ORDER.value]
                ]

                assert len(app_rate_events) == 2
                assert len(delivery_rate_events) == 1
                assert len(item_rate_events) > 0

        assert_events_have_correct_schema(events)


def test_order_delivery_no_weekends():
    global_conf.sink_types = ['memory']
    global_conf.start_ts = datetime(2001, 1, 5)
    global_conf.profiles["boring_guy"].session_engagement = EngagementConfig(
        initial_min=1.0,
        initial_max=1.0,
        change_probability=1.0,
        boost_probability=0.0,
        decay_probability=1.0,
        change_min=1.0,
        change_max=1.0,
    )
    global_conf.profiles["boring_guy"].user_type = SyntheticUserType.PURCHASE_ENGAGEMENT
    global_conf.profiles["boring_guy"].behaviour.schedule.delivery_delay_max_days = 0

    global_conf.profiles["boring_guy"].behaviour.purchase.checkout_failure_probability_min = 0.0
    global_conf.profiles["boring_guy"].behaviour.purchase.checkout_failure_probability_max = 0.0
    global_conf.profiles["boring_guy"].behaviour.purchase.checkout_urgent_probability_min = 0.0
    global_conf.profiles["boring_guy"].behaviour.purchase.checkout_urgent_probability_max = 0.0
    global_conf.profiles["boring_guy"].behaviour.purchase.checkout_cancellation_probability_min = 0.0
    global_conf.profiles["boring_guy"].behaviour.purchase.checkout_cancellation_probability_max = 0.0

    global_conf.profiles["boring_guy"].configure_early_start()
    global_conf.profiles["boring_guy"].behaviour.purchase.update_events_per_checkout_min = 20
    global_conf.profiles["boring_guy"].behaviour.purchase.update_events_per_checkout_max = 20

    assert global_conf.profiles["boring_guy"].behaviour.purchase.interest_catalog_range_min == 0.1
    assert global_conf.start_ts.weekday() == 4, "Need to start this experiment on a Friday!"

    for day_index in range(0, 4):
        global_conf.end_ts = global_conf.start_ts + timedelta(days=day_index, hours=23, minutes=59)

        driver = Driver(clear_cache_after_flush=True)
        driver.run()

        events = driver.get_and_clear_memory_sink_events()
        delivery_events = [event for event in events.log_events if isinstance(event, DeliveryEvent)]
        schedule_delivery_events = [
            event
            for event in events.log_events
            if isinstance(event, ScheduleDeliveryEvent)
            and event.props['action'] == ScheduleDeliveryAction.SCHEDULE.value
        ]
        update_delivery_events = [
            event
            for event in events.log_events
            if isinstance(event, ScheduleDeliveryEvent) and event.props['action'] == ScheduleDeliveryAction.UPDATE.value
        ]
        payment_method_events = [event for event in events.log_events if isinstance(event, PaymentMethodEvent)]

        assert len(schedule_delivery_events) > 0
        assert len(payment_method_events) > 0

        if day_index == 0:
            assert driver.last_seen_ts.weekday() == 4  # This is a Friday

            # Delivery is scheduled, even though it won't actually happen until Monday
            scheduled_deliveries = driver.get_scheduled_order_deliveries()
            assert len(scheduled_deliveries) == 1
            assert datetime.fromtimestamp(scheduled_deliveries[0]["delivery_timestamp"]).date() == date(2001, 1, 8)

            # No deliveries, yet
            assert len(delivery_events) == 0
            assert len(update_delivery_events) > 0
        elif day_index < 2:
            assert len(delivery_events) == 0
        elif day_index < 3:
            assert len(delivery_events) == 0
        else:
            assert global_conf.end_ts.weekday() == 0  # Monday
            # All at once!
            assert len(delivery_events) >= 2

        assert_events_have_correct_schema(events)


@pytest.mark.parametrize("all_cancelled", [True, False])
def test_order_delivery_urgent(all_cancelled):
    global_conf.sink_types = ["memory"]
    global_conf.start_ts = datetime(2001, 1, 5)
    global_conf.profiles["boring_guy"].session_engagement = EngagementConfig(
        initial_min=1.0,
        initial_max=1.0,
        change_probability=1.0,
        boost_probability=0.0,
        decay_probability=1.0,
        change_min=1.0,
        change_max=1.0,
    )
    global_conf.profiles["boring_guy"].user_type = SyntheticUserType.PURCHASE_ENGAGEMENT
    global_conf.profiles["boring_guy"].behaviour.schedule.delivery_delay_max_days = 0

    global_conf.profiles["boring_guy"].configure_early_start()
    global_conf.profiles["boring_guy"].behaviour.purchase.checkout_failure_probability_min = 0.0
    global_conf.profiles["boring_guy"].behaviour.purchase.checkout_failure_probability_max = 0.0
    global_conf.profiles["boring_guy"].behaviour.purchase.checkout_urgent_probability_min = 1.0
    global_conf.profiles["boring_guy"].behaviour.purchase.checkout_urgent_probability_max = 1.0
    global_conf.profiles["boring_guy"].behaviour.purchase.views_required_per_purchase_min = 2
    global_conf.profiles["boring_guy"].behaviour.purchase.views_required_per_purchase_max = 2
    global_conf.profiles["boring_guy"].behaviour.purchase.interest_catalog_range_min = 1.0
    global_conf.profiles["boring_guy"].behaviour.purchase.interest_catalog_range_max = 1.0
    global_conf.profiles["boring_guy"].behaviour.purchase.interest_per_item_min = 1.0
    global_conf.profiles["boring_guy"].behaviour.purchase.interest_per_item_max = 1.0

    if all_cancelled:
        global_conf.profiles["boring_guy"].behaviour.purchase.checkout_cancellation_probability_min = 1.0
        global_conf.profiles["boring_guy"].behaviour.purchase.checkout_cancellation_probability_max = 1.0
    else:
        global_conf.profiles["boring_guy"].behaviour.purchase.checkout_cancellation_probability_min = 0.0
        global_conf.profiles["boring_guy"].behaviour.purchase.checkout_cancellation_probability_max = 0.0

    global_conf.profiles["boring_guy"].behaviour.purchase.update_events_per_checkout_min = 2
    global_conf.profiles["boring_guy"].behaviour.purchase.update_events_per_checkout_max = 2

    assert global_conf.profiles["boring_guy"].behaviour.purchase.interest_catalog_range_min == 1.0
    assert global_conf.start_ts.weekday() == 4, "Need to start this experiment on a Friday!"

    all_cancellation_events = []
    for day_index in range(0, 4):
        global_conf.end_ts = global_conf.start_ts + timedelta(days=day_index, hours=23, minutes=59)

        driver = Driver(clear_cache_after_flush=True)
        driver.run()

        events = driver.get_and_clear_memory_sink_events()
        assert_events_have_correct_schema(events)

        assert len(events.log_events) > 0

        delivery_events = [event for event in events.log_events if isinstance(event, DeliveryEvent)]
        schedule_delivery_events = [
            event
            for event in events.log_events
            if isinstance(event, ScheduleDeliveryEvent)
            and event.props['action'] == ScheduleDeliveryAction.SCHEDULE.value
        ]
        update_delivery_events = [
            event
            for event in events.log_events
            if isinstance(event, ScheduleDeliveryEvent) and event.props['action'] == ScheduleDeliveryAction.UPDATE.value
        ]
        cancel_order_events = [event for event in events.log_events if isinstance(event, CancelCheckoutEvent)]
        payment_method_events = [event for event in events.log_events if isinstance(event, PaymentMethodEvent)]

        scheduled_deliveries = driver.get_scheduled_order_deliveries()
        scheduled_cancellations = driver.get_scheduled_order_cancellations()

        if all_cancelled:
            assert len(delivery_events) == 0 and len(scheduled_deliveries) == 0
            assert len(cancel_order_events) > 0 or len(scheduled_cancellations) > 0
            all_cancellation_events.extend(cancel_order_events)
        else:
            assert len(delivery_events) > 0 or len(scheduled_deliveries) > 0
            assert len(cancel_order_events) == 0 and len(scheduled_cancellations) == 0

        assert all([event.props['is_urgent'] for event in schedule_delivery_events])
        if day_index > 0:
            if all_cancelled:
                assert len(delivery_events) == 0, "No deliveries on day %s!" % (day_index,)
            else:
                assert len(schedule_delivery_events) > 0, "No scheduling on day %s!" % (day_index,)
                assert len(payment_method_events) > 0
                assert len(update_delivery_events) > 0, "No updates on day %s!" % (day_index,)
                assert len(delivery_events) > 0, "No deliveries on day %s!" % (day_index,)

    if all_cancelled:
        for cancel_type in CancelType:
            assert (
                len(
                    [
                        cancel_event
                        for cancel_event in all_cancellation_events
                        if cancel_event.props["type"] == cancel_type.value
                    ]
                )
                > 0
            ), f"{cancel_type} not found!"


def test_caching_logs_to_disk():
    catalog_types = [CatalogType.BLOOD, CatalogType.DRUG, CatalogType.OXYGEN, CatalogType.MEDICAL_EQUIPMENT]

    global_conf.start_ts = datetime(2001, 1, 5)
    global_conf.use_promotions = True
    for catalog_type in catalog_types:
        global_conf.catalogs[catalog_type] = CatalogConfig(
            target_count=30,
        )

    cost_adjustment_ratio_min = 0.8
    cost_adjustment_ratio_max = 0.9
    item_count_min = 3
    item_count_max = 30
    global_conf.catalogs[CatalogType.PROMO] = CatalogConfig(
        target_count=5,
        properties={
            "length_min_days": 1,
            "length_max_days": 2,
            "cost_adjustment_min_ratio": cost_adjustment_ratio_min,
            "cost_adjustment_max_ratio": cost_adjustment_ratio_max,
            "item_min_count": item_count_min,
            "item_max_count": item_count_max,
        },
    )

    profile_conf = global_conf.profiles["boring_guy"]
    profile_conf.configure_early_start()
    profile_conf.session_engagement = EngagementConfig(
        initial_min=1.0,
        initial_max=1.0,
        change_probability=1.0,
        boost_probability=0.0,
        decay_probability=1.0,
        change_min=1.0,
        change_max=1.0,
    )
    profile_conf.user_type = SyntheticUserType.PURCHASE_ENGAGEMENT
    profile_conf.behaviour.purchase.catalog_type_probabilities = dict(
        [(catalog_type, 1.0) for catalog_type in catalog_types]
    )

    profile_conf.behaviour.schedule.delivery_delay_max_days = 0

    profile_conf.behaviour.purchase.checkout_promo_probability_min = 1.0
    profile_conf.behaviour.purchase.checkout_promo_probability_max = 1.0
    profile_conf.behaviour.purchase.checkout_failure_probability_min = 0.0
    profile_conf.behaviour.purchase.checkout_failure_probability_max = 0.0
    profile_conf.behaviour.purchase.checkout_urgent_probability_min = 1.0
    profile_conf.behaviour.purchase.checkout_urgent_probability_max = 1.0
    profile_conf.behaviour.purchase.checkout_cancellation_probability_min = 0.0
    profile_conf.behaviour.purchase.checkout_cancellation_probability_max = 0.0

    profile_conf.behaviour.purchase.update_events_per_checkout_min = 2
    profile_conf.behaviour.purchase.update_events_per_checkout_max = 2

    assert profile_conf.behaviour.purchase.interest_catalog_range_min == 0.1
    if os.path.exists(Driver.get_cache_filename()):
        os.remove(Driver.get_cache_filename())

    for day_index in range(0, 4):
        global_conf.end_ts = global_conf.start_ts + timedelta(days=day_index, hours=23, minutes=59)

        driver = Driver(clear_cache_after_flush=False)
        driver.run()

        pre_flushed_events = driver.get_cached_events()
        driver.persist_cache_to_disk()
        driver.clear_cache()
        assert len(driver.get_cached_log_events()) == 0
        assert os.path.exists(driver.get_cache_filename())
        driver.restore_cache_from_disk()
        assert len(driver.get_cached_log_events()) > 0
        assert not os.path.exists(driver.get_cache_filename())
        post_flushed_events = driver.get_cached_events()

        assert len(pre_flushed_events.log_events) == len(post_flushed_events.log_events)
        assert len(pre_flushed_events.catalog_events) == len(post_flushed_events.catalog_events)
        assert len(pre_flushed_events.meta_events) == len(post_flushed_events.meta_events)


def test_orders_with_promotions():
    catalog_types = [CatalogType.BLOOD, CatalogType.DRUG, CatalogType.OXYGEN, CatalogType.MEDICAL_EQUIPMENT]

    global_conf.start_ts = datetime(2001, 1, 5)
    global_conf.use_promotions = True
    for catalog_type in catalog_types:
        global_conf.catalogs[catalog_type] = CatalogConfig(
            target_count=30,
        )

    cost_adjustment_ratio_min = 0.8
    cost_adjustment_ratio_max = 0.9
    item_count_min = 3
    item_count_max = 30
    global_conf.catalogs[CatalogType.PROMO] = CatalogConfig(
        target_count=5,
        properties={
            "length_min_days": 1,
            "length_max_days": 2,
            "cost_adjustment_min_ratio": cost_adjustment_ratio_min,
            "cost_adjustment_max_ratio": cost_adjustment_ratio_max,
            "item_min_count": item_count_min,
            "item_max_count": item_count_max,
        },
    )

    profile_conf = global_conf.profiles["boring_guy"]
    profile_conf.configure_early_start()
    profile_conf.session_engagement = EngagementConfig(
        initial_min=1.0,
        initial_max=1.0,
        change_probability=1.0,
        boost_probability=0.0,
        decay_probability=1.0,
        change_min=1.0,
        change_max=1.0,
    )
    profile_conf.user_type = SyntheticUserType.PURCHASE_ENGAGEMENT
    profile_conf.behaviour.purchase.catalog_type_probabilities = dict(
        [(catalog_type, 1.0) for catalog_type in catalog_types]
    )

    profile_conf.behaviour.schedule.delivery_delay_max_days = 0

    profile_conf.behaviour.purchase.checkout_promo_probability_min = 1.0
    profile_conf.behaviour.purchase.checkout_promo_probability_max = 1.0
    profile_conf.behaviour.purchase.checkout_failure_probability_min = 0.0
    profile_conf.behaviour.purchase.checkout_failure_probability_max = 0.0
    profile_conf.behaviour.purchase.checkout_urgent_probability_min = 1.0
    profile_conf.behaviour.purchase.checkout_urgent_probability_max = 1.0
    profile_conf.behaviour.purchase.checkout_cancellation_probability_min = 0.0
    profile_conf.behaviour.purchase.checkout_cancellation_probability_max = 0.0

    profile_conf.behaviour.purchase.update_events_per_checkout_min = 2
    profile_conf.behaviour.purchase.update_events_per_checkout_max = 2

    assert profile_conf.behaviour.purchase.interest_catalog_range_min == 0.1

    ever_used_promotions = False
    driver_meta_id: Optional[int] = None
    for day_index in range(0, 4):
        global_conf.end_ts = global_conf.start_ts + timedelta(days=day_index, hours=23, minutes=59)

        driver = Driver(clear_cache_after_flush=True)
        driver.run()

        events = driver.get_and_clear_memory_sink_events()

        checkout_events = [event for event in events.log_events if isinstance(event, CheckoutEvent)]
        assert len(checkout_events) > 0
        promos_used_count = 0
        for checkout_event in checkout_events:
            for item_data in checkout_event.props["items"]:
                item_catalog = CatalogCache.get_catalog_by_uuid(CatalogType(item_data['type']), item_data['id'])

                if "promo_id" not in item_data or item_data["promo_id"] is None:
                    assert abs(item_data['price'] / item_data['quantity'] - item_catalog['item_price']) < 10e-3
                else:
                    assert item_data['price'] / item_data['quantity'] < item_catalog['item_price']
                    promos_used_count += 1

        if promos_used_count > 0:
            promo_apply_events = [
                event
                for event in events.log_events
                if isinstance(event, PromoEvent) and event.props["action"] == PromoAction.APPLY.value
            ]
            assert len(promo_apply_events) > 0
            promo_view_events = [
                event
                for event in events.log_events
                if isinstance(event, PromoEvent) and event.props["action"] == PromoAction.VIEW.value
            ]
            assert len(promo_view_events) > 0

            ever_used_promotions = True

        if driver_meta_id is None:
            driver_meta_id = driver.driver_meta_id

        # We should have 5 active promos at any given moment
        cached_catalogs = CatalogCache.cached_catalog[CatalogType.PROMO]
        assert len(cached_catalogs) == 5
        promotion_uuids = []
        for promo in cached_catalogs.values():
            assert promo['start_timestamp'] <= driver.last_seen_ts.timestamp()
            assert promo['end_timestamp'] >= driver.last_seen_ts.timestamp()
            promotion_uuids.append(promo['uuid'])

        assert_events_have_correct_schema(events)

        assert len(CatalogCache.current_promotions) > 0
        for item_type, promotions_by_item in CatalogCache.current_promotions.items():
            assert item_type in list(ItemType)
            for item_uuid, promotion_tuples in promotions_by_item.items():
                assert len(promotion_tuples) > 0
                for promotion_uuid, cost_adjustment_ratio in promotion_tuples:
                    assert CatalogCache.get_catalog_by_uuid(CatalogType(item_type.value), item_uuid) is not None
                    assert cost_adjustment_ratio_min <= cost_adjustment_ratio <= cost_adjustment_ratio_max

    assert ever_used_promotions
    assert driver_meta_id is not None
    with create_db_session() as db_session:
        promos_in_db = (
            db_session.query(CatalogEntrySchema)
            .filter_by(driver_meta_id=driver_meta_id, type=CatalogType.PROMO.value)
            .all()
        )
        # We should have catalogs in DB
        assert len(promos_in_db) > 0
        # Some of them should have expired
        assert len(promos_in_db) > len(CatalogCache.cached_catalog[CatalogType.PROMO])

        for promo in promos_in_db:
            assert promo.data["uuid"] is not None
            assert cost_adjustment_ratio_min <= promo.data['cost_adjustment_ratio'] <= cost_adjustment_ratio_max
            assert item_count_min <= len(promo.data['promoted_item_uuids']) <= item_count_max
            assert len(promo.data['promoted_item_uuids']) == len(promo.data['promoted_item_types'])
