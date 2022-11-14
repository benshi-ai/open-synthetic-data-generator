import random
import pytest

from datetime import datetime, timedelta

from synthetic.catalog.cache import CatalogCache
from synthetic.conf import EngagementConfig, ProfileConfig, global_conf
from synthetic.constants import ProductUserType
from synthetic.event.constants import EventType
from synthetic.event.log.general.rate import RateEvent
from synthetic.event.log.navigation.app import AppEvent
from synthetic.user.session_engagement_user import SessionEngagementUser
from synthetic.utils.test_utils import assert_events_have_correct_schema


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.profiles = {
        "mobile_guy": ProfileConfig(
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
            product_user_type=ProductUserType.MOBILE,
        )
    }


@pytest.fixture
def registration_ts():
    return datetime(2000, 1, 1, 0, 0, 0)


@pytest.fixture
def mobile_user(driver_meta, registration_ts):
    return SessionEngagementUser.create_random_user(driver_meta.id, registration_ts, profile_name="mobile_guy")


def test_single_session_with_app(db_session, registration_ts, mobile_user):
    global_conf.rating_probability = 1.0

    random.seed(0)

    CatalogCache.warm_up(db_session=db_session)

    end_ts = registration_ts + timedelta(days=1)
    events = mobile_user.generate_events(end_ts)
    log_events = events.log_events
    log_events.extend(mobile_user.get_scheduled_log_events())

    assert_events_have_correct_schema(events)

    app_events = [event for event in log_events if isinstance(event, AppEvent)]
    assert len(app_events) == 2

    rate_events = [event for event in log_events if isinstance(event, RateEvent)]
    assert len(rate_events) > 0
