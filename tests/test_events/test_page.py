import random
import pytest

from datetime import datetime, timedelta

from synthetic.catalog.cache import CatalogCache
from synthetic.conf import EngagementConfig, ProfileConfig, global_conf, EventConfig
from synthetic.event.catalog.user_catalog import UserCatalogEvent
from synthetic.event.constants import EventType
from synthetic.event.log.navigation.identify import IdentifyEvent
from synthetic.event.log.general.page import PageEvent
from synthetic.user.session_engagement_user import SessionEngagementUser
from synthetic.utils.test_utils import assert_dicts_equal_partial, assert_events_have_correct_schema


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.profiles = {
        "page_guy": ProfileConfig(
            session_engagement=EngagementConfig(
                initial_min=1.0,
                initial_max=1.0,
            ),
            occurrence_probability=1.0,
            session_min_count=1,
            session_max_count=1,
            session_event_type_changes_min=0,
            session_event_type_changes_max=0,
            session_length_min_seconds=60,
            session_length_max_seconds=60,
            online_probability=0.5,
            event_probabilities={EventType.PAGE: 1.0},
            events={EventType.PAGE: EventConfig(properties={"duration_seconds_min": 60, "duration_seconds_max": 60})},
        )
    }


@pytest.fixture
def registration_ts():
    return datetime(2000, 1, 1, 0, 0, 0)


@pytest.fixture
def page_user(driver_meta, registration_ts):
    return SessionEngagementUser.create_random_user(driver_meta.id, registration_ts, profile_name="page_guy")


def test_single_session_with_multiple_pages(db_session, registration_ts, page_user):
    random.seed(0)
    CatalogCache.warm_up(db_session)

    end_ts = registration_ts + timedelta(days=1)
    events = page_user.generate_events(end_ts)
    log_events = sorted(events.log_events, key=lambda event: event.ts)
    catalog_events = events.catalog_events

    user_catalog_events = [event for event in catalog_events if isinstance(event, UserCatalogEvent)]
    assert len(user_catalog_events) == 1

    assert_dicts_equal_partial(user_catalog_events[0].as_payload_dict(), page_user.get_all_user_data())

    assert page_user.session_engagement_level == 1.0

    page_events = [event for event in log_events if isinstance(event, PageEvent)]
    assert len(page_events) > 0

    # We registered and logged in and out also
    assert len([event for event in log_events if isinstance(event, IdentifyEvent)]) == 3

    # Check register
    register_event = log_events[0]
    assert_dicts_equal_partial(register_event.props, {"action": "register"})
    # Should register before login
    assert register_event.ts < log_events[1].ts

    # Check login and logout
    login_event = log_events[1]
    assert_dicts_equal_partial(login_event.props, {"action": "login"})

    # Should login before the page views
    assert login_event.ts < min([page_event.ts for page_event in page_events])

    logout_event = log_events[-1]
    assert_dicts_equal_partial(logout_event.props, {"action": "logout"})
    # Should logout after the page views
    assert logout_event.ts > max([page_event.ts for page_event in page_events])

    seen_pages = set()
    last_ts = registration_ts
    for event in page_events:
        assert registration_ts <= event.ts <= end_ts
        assert event.ts > last_ts

        assert event.props["path"] not in seen_pages

        seen_pages.add(event.props["path"])
        last_ts = event.ts

    assert_events_have_correct_schema(events)


def test_manage_schedule_end_ts(db_session, registration_ts, page_user):
    # Session starts at the end of the day and will go long
    global_conf.profiles["page_guy"].session_hourly_start_probabilities = [0.0] * 24
    global_conf.profiles["page_guy"].session_hourly_start_probabilities[23] = 1.0
    global_conf.profiles["page_guy"].session_length_min_seconds = 3600 * 2
    global_conf.profiles["page_guy"].session_length_max_seconds = 3600 * 2

    random.seed(0)
    CatalogCache.warm_up(db_session)

    end_ts = registration_ts + timedelta(days=1)
    events = page_user.generate_events(end_ts)
    assert max([log_event.ts for log_event in events.log_events]) < end_ts

    max_schedule_event = max([log_event.ts for log_event in page_user.get_scheduled_events().log_events])
    assert max_schedule_event > end_ts
    assert max_schedule_event <= page_user.get_schedule_end_ts()
