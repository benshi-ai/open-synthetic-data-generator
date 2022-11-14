import random
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from mock import mock

from synthetic.catalog.cache import CatalogCache
from synthetic.conf import global_conf, ProfileConfig, EventConfig, EngagementConfig
from synthetic.constants import ProductUserType
from synthetic.event.constants import EventType
from synthetic.event.log.navigation.app import AppAction
from synthetic.event.log.navigation.identify import IdentifyAction
from synthetic.event.meta.receive_nudge import ReceiveNudges
from synthetic.user.factory import store_user_in_db, load_user_from_db
from synthetic.user.session_engagement_user import SessionEngagementUser
from synthetic.utils.event_utils import calculate_bonus_session_count
from synthetic.utils.nudge_utils import Nudge
from synthetic.utils.test_utils import assert_events_have_correct_schema, assert_dicts_equal_partial


@pytest.fixture(autouse=True)
def fixed_seed():
    random.seed(0)


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.profiles = {
        "boring_guy": ProfileConfig(
            occurrence_probability=1.0,
            session_min_count=5,
            session_max_count=5,
            online_probability=0.5,
            session_engagement=EngagementConfig(
                initial_min=1.0,
                initial_max=1.0,
                change_min=0.2,
                change_max=0.2,
                change_probability=1.0,
                decay_probability=1.0,
                boost_probability=0.0,
            ),
            event_probabilities={EventType.PAGE: 1.0},
        )
    }


@pytest.fixture()
def profile_name():
    profile_name = "boring_guy"

    return profile_name


def test_generate_batch_page_logs_new_user(db_session, driver_meta, profile_name):
    CatalogCache.warm_up(db_session)

    start_ts = datetime(2000, 1, 1)
    end_ts = datetime(2000, 1, 2)
    global_conf.start_ts = start_ts

    user = SessionEngagementUser(
        driver_meta.id,
        str(uuid4()),
        profile_data={
            "profile_name": profile_name,
            "registration_timestamp": start_ts.timestamp(),
        },
    )

    events = user.generate_events(end_ts)
    log_events = sorted(events.log_events, key=lambda event: event.ts)

    event_ts_list = [event.ts for event in log_events]
    assert min(event_ts_list) >= datetime(2000, 1, 1)
    assert max(event_ts_list) <= datetime(2000, 1, 2)

    assert user.get_manager("session_engagement").get_engagement() == 0.8
    assert_dicts_equal_partial(
        user.get_profile_data(),
        {
            "active_dates": {"2000-01-01": True},
            "managers": {
                "session_engagement": {
                    "engagement_level": 0.8,
                    "last_seen_ts": end_ts.timestamp(),
                }
            },
            "profile_name": profile_name,
            "registration_timestamp": start_ts.timestamp(),
        },
    )
    assert user.last_seen_ts == end_ts

    store_user_in_db(db_session, driver_meta.id, user)

    loaded_user = load_user_from_db(db_session, driver_meta.id, user.get_platform_uuid())
    assert loaded_user.get_manager("session_engagement").get_engagement() == 0.8


def test_generate_nudge_check_events(db_session, driver_meta, profile_name):
    CatalogCache.warm_up(db_session)

    start_ts = datetime(2000, 1, 1)
    end_ts = datetime(2000, 1, 2)
    global_conf.start_ts = start_ts
    global_conf.use_nudges = True
    global_conf.artificial_nudge_min_registration_delay_days = 0
    profile_conf = global_conf.profiles[profile_name]
    profile_conf.nudges.checks_per_day_min = 1
    profile_conf.nudges.checks_per_day_max = 1

    user = SessionEngagementUser(
        driver_meta.id,
        str(uuid4()),
        profile_data={
            "profile_name": profile_name,
            "registration_timestamp": start_ts.timestamp(),
        },
    )

    events = user.generate_events(end_ts)
    meta_events = events.meta_events

    assert len(meta_events) == 3
    assert isinstance(sorted(meta_events, key=lambda event: event.ts)[1], ReceiveNudges)


@pytest.mark.parametrize("batch", [True, False])
def test_generate_incremental_page_logs_new_user(db_session, driver_meta, profile_name, batch):
    CatalogCache.warm_up(db_session)

    start_ts = datetime(2000, 1, 1)
    end_ts = datetime(2000, 1, 2)

    profile_conf = global_conf.profiles[profile_name]
    profile_conf.events[EventType.PAGE] = EventConfig(
        properties={
            "page_count_per_session_min": 100,
            "page_count_per_session_max": 100,
            "duration_seconds_min": 36 * 12,
            "duration_seconds_max": 36 * 12,
        }
    )
    profile_conf.session_min_count = 1
    profile_conf.session_max_count = 1
    profile_conf.session_event_type_changes_min = 0
    profile_conf.session_event_type_changes_max = 0
    profile_conf.session_length_min_seconds = 3600 * 12
    profile_conf.session_length_max_seconds = 3600 * 12
    profile_conf.configure_early_start()

    user = SessionEngagementUser(
        driver_meta.id,
        str(uuid4()),
        profile_data={
            "profile_name": profile_name,
            "registration_timestamp": start_ts.timestamp(),
        },
    )

    assert len(user.get_scheduled_log_events()) == 0
    if batch:
        events = user.generate_events(end_ts)
        log_events = events.log_events
    else:
        log_events = []
        last_current_ts = None
        current_ts = start_ts + timedelta(seconds=3600)
        while current_ts < end_ts:
            partial_events = user.generate_events(current_ts)
            partial_log_events = sorted(partial_events.log_events, key=lambda event: event.ts)
            assert sorted(partial_log_events, key=lambda event: event.ts) == partial_log_events
            assert 0 <= len(partial_log_events) <= 30
            if len(partial_log_events) > 0 and last_current_ts is not None:
                assert last_current_ts <= min([event.ts for event in partial_log_events])
                assert max([event.ts for event in partial_log_events]) <= current_ts

            log_events.extend(partial_log_events)

            last_current_ts = current_ts
            current_ts += timedelta(seconds=3600)

    assert len(log_events) >= 103


def test_generate_app_logs_new_user(db_session, driver_meta, profile_name):
    CatalogCache.warm_up(db_session)

    start_ts = datetime(2000, 1, 1)
    end_ts = datetime(2000, 1, 2)

    profile_conf = global_conf.profiles[profile_name]
    profile_conf.events[EventType.PAGE] = EventConfig(
        properties={
            "page_count_per_session_min": 100,
            "page_count_per_session_max": 100,
            "duration_seconds_min": 36 * 12,
            "duration_seconds_max": 36 * 12,
        }
    )
    profile_conf.product_user_type = ProductUserType.MOBILE
    profile_conf.background_per_minute_probability = 1.0
    profile_conf.session_min_count = 1
    profile_conf.session_max_count = 1
    profile_conf.session_event_type_changes_min = 0
    profile_conf.session_event_type_changes_max = 0
    profile_conf.session_length_min_seconds = 3600 * 12
    profile_conf.session_length_max_seconds = 3600 * 12
    profile_conf.configure_early_start()

    user = SessionEngagementUser(
        driver_meta.id,
        str(uuid4()),
        profile_data={
            "profile_name": profile_name,
            "registration_timestamp": start_ts.timestamp(),
        },
    )

    assert len(user.get_scheduled_log_events()) == 0
    events = user.generate_events(end_ts)
    log_events = events.log_events

    assert_events_have_correct_schema(events)

    expected_background_event_count = 833
    login_events = [
        event
        for event in log_events
        if event.event_type == "identify" and event.props["action"] == IdentifyAction.LOGIN.value
    ]
    assert len(login_events) == expected_background_event_count + 1

    app_events = [event for event in log_events if event.event_type == "app"]

    open_events = [event for event in app_events if event.props["action"] == AppAction.OPEN.value]
    assert len(open_events) == 1

    close_events = [event for event in app_events if event.props["action"] == AppAction.CLOSE.value]
    assert len(close_events) == 1

    background_events = [event for event in app_events if event.props["action"] == AppAction.BACKGROUND.value]
    assert len(background_events) == expected_background_event_count

    resume_events = [event for event in app_events if event.props["action"] == AppAction.RESUME.value]
    assert len(resume_events) == expected_background_event_count


def test_generate_logs_on_single_day(db_session, driver_meta, profile_name):
    CatalogCache.warm_up(db_session)

    start_ts = datetime(2000, 1, 1)
    end_ts = datetime(2000, 1, 8)

    profile_conf = global_conf.profiles[profile_name]
    profile_conf.events[EventType.PAGE] = EventConfig(
        properties={
            "page_count_per_session_min": 1,
            "page_count_per_session_max": 1,
            "duration_seconds_min": 10,
            "duration_seconds_max": 10,
        }
    )
    profile_conf.session_engagement.change_probability = 0.0
    profile_conf.product_user_type = ProductUserType.MOBILE
    profile_conf.background_per_minute_probability = 1.0
    profile_conf.session_min_count = 1
    profile_conf.session_max_count = 1
    profile_conf.session_event_type_changes_min = 0
    profile_conf.session_event_type_changes_max = 0
    profile_conf.session_length_min_seconds = 10
    profile_conf.session_length_max_seconds = 10
    profile_conf.session_day_of_week_probabilities = [0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0]

    user = SessionEngagementUser(
        driver_meta.id,
        str(uuid4()),
        profile_data={
            "profile_name": profile_name,
            "registration_timestamp": start_ts.timestamp(),
        },
    )

    assert len(user.get_scheduled_log_events()) == 0
    events = user.generate_events(end_ts)
    log_events = events.log_events

    assert_events_have_correct_schema(events)

    login_events = [
        event
        for event in log_events
        if event.event_type == "identify" and event.props["action"] == IdentifyAction.LOGIN.value
    ]
    all_event_dow = [log_event.ts.weekday() for log_event in login_events]
    assert len(all_event_dow) > 0
    assert all([dow == 2 for dow in all_event_dow])


@mock.patch("synthetic.user.synthetic_user.get_nudges_from_backend")
def test_behaviour_changes_directly_after_nudge(m_get_nudges, db_session, driver_meta, profile_name):
    CatalogCache.warm_up(db_session)

    global_conf.use_nudges = True
    global_conf.artificial_nudge_min_registration_delay_days = 0

    profile_conf = global_conf.profiles[profile_name]
    profile_conf.session_min_count = 1
    profile_conf.session_max_count = 1
    profile_conf.session_length_min_seconds = 60
    profile_conf.session_length_max_seconds = 60
    profile_conf.nudges.bonus_session_count = 100
    profile_conf.nudges.bonus_session_days = 100

    profile_conf.events[EventType.PAGE] = EventConfig(
        properties={
            "page_count_per_session_min": 1,
            "page_count_per_session_max": 1,
            "duration_seconds_min": 60,
            "duration_seconds_max": 60,
        }
    )

    start_ts = datetime(2000, 1, 1)
    user = SessionEngagementUser(
        driver_meta.id,
        str(uuid4()),
        profile_data={
            "profile_name": profile_name,
            "registration_timestamp": start_ts.timestamp(),
        },
    )

    user.set_last_seen_ts(start_ts)
    nudge_ts = start_ts + timedelta(hours=12)
    end_ts = start_ts + timedelta(days=1)
    pre_events = user.generate_events(nudge_ts)

    m_get_nudges.return_value = [Nudge(nudge_id=1, subject_id=user.get_platform_uuid(), queued_at=nudge_ts)]
    user.retrieve_and_receive_nudges(nudge_ts)

    post_events = user.generate_events(end_ts)

    assert len(post_events.log_events) > len(pre_events.log_events) * 20


def test_bonus_session_count():
    # Single day
    assert (
        calculate_bonus_session_count(
            datetime(2000, 1, 1), datetime(2000, 1, 1, 0, 0, 0), bonus_session_count=1, bonus_session_days=1
        )
        == 1
    )
    assert (
        calculate_bonus_session_count(
            datetime(2000, 1, 1), datetime(2000, 1, 1, 12, 0, 0), bonus_session_count=1, bonus_session_days=1
        )
        == 1
    )
    assert (
        calculate_bonus_session_count(
            datetime(2000, 1, 1), datetime(2000, 1, 1, 23, 59, 59), bonus_session_count=1, bonus_session_days=1
        )
        == 1
    )
    assert (
        calculate_bonus_session_count(
            datetime(2000, 1, 1), datetime(2000, 1, 2, 0, 0, 0), bonus_session_count=1, bonus_session_days=1
        )
        == 0
    )

    # Seven days
    assert (
        calculate_bonus_session_count(
            datetime(2002, 1, 1), datetime(2000, 1, 1), bonus_session_count=7, bonus_session_days=7
        )
        == 0
    )
    assert (
        calculate_bonus_session_count(
            datetime(2001, 1, 1), datetime(2000, 1, 1), bonus_session_count=7, bonus_session_days=7
        )
        == 0
    )
    assert (
        calculate_bonus_session_count(
            datetime(2001, 1, 1), datetime(2001, 1, 1), bonus_session_count=7, bonus_session_days=7
        )
        == 7
    )
    assert (
        calculate_bonus_session_count(
            datetime(2001, 1, 1), datetime(2001, 1, 3), bonus_session_count=7, bonus_session_days=7
        )
        == 7
    )
    assert (
        calculate_bonus_session_count(
            datetime(2001, 1, 1), datetime(2001, 1, 7), bonus_session_count=7, bonus_session_days=7
        )
        == 7
    )
    assert (
        calculate_bonus_session_count(
            datetime(2001, 1, 1), datetime(2001, 1, 8), bonus_session_count=7, bonus_session_days=7
        )
        == 0
    )
    assert (
        calculate_bonus_session_count(
            datetime(2001, 1, 1), datetime(2001, 1, 9), bonus_session_count=7, bonus_session_days=7
        )
        == 0
    )
    assert (
        calculate_bonus_session_count(
            datetime(2001, 1, 1), datetime(2002, 1, 1), bonus_session_count=7, bonus_session_days=7
        )
        == 0
    )
