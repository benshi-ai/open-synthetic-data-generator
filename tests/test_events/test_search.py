import random
import pytest

from datetime import datetime, timedelta

from synthetic.catalog.cache import CatalogCache
from synthetic.conf import EngagementConfig, ProfileConfig, global_conf, EventConfig
from synthetic.event.constants import EventType
from synthetic.event.log.general.search import SearchEvent
from synthetic.user.session_engagement_user import SessionEngagementUser
from synthetic.utils.test_utils import assert_events_have_correct_schema


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.profiles = {
        "search_guy": ProfileConfig(
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
            event_probabilities={EventType.SEARCH: 1.0},
            events={
                EventType.SEARCH: EventConfig(
                    properties={
                        "duration_seconds_min": 60,
                        "duration_seconds_max": 60,
                        "results_per_page_max": 5,
                        "page_count_per_session_min": 5,
                        "page_count_per_session_max": 5,
                    }
                )
            },
        )
    }


@pytest.fixture
def registration_ts():
    return datetime(2000, 1, 1, 0, 0, 0)


@pytest.fixture
def search_user(driver_meta, registration_ts):
    return SessionEngagementUser.create_random_user(driver_meta.id, registration_ts, profile_name="search_guy")


def test_single_session_with_multiple_search_pages(db_session, registration_ts, search_user):
    random.seed(0)

    CatalogCache.warm_up(db_session)

    end_ts = registration_ts + timedelta(days=1)
    events = search_user.generate_events(end_ts)
    log_events = sorted(events.log_events, key=lambda event: event.ts)

    impression_events = [
        event for event in log_events if event.event_type == "item" and event.props["action"] == "impression"
    ]
    search_events = [event for event in log_events if isinstance(event, SearchEvent)]
    assert len(impression_events) > 20
    assert len(search_events) == 5

    for search_event in search_events[0:-1]:
        assert len(search_event.props["results_list"]) == 5
    assert len(search_events[-1].props["results_list"]) > 0

    assert_events_have_correct_schema(events)
