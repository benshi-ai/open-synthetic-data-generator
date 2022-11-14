import random
import pytest

from datetime import datetime, timedelta

from synthetic.catalog.cache import CatalogCache
from synthetic.conf import EngagementConfig, ProfileConfig, global_conf, EventConfig
from synthetic.event.constants import EventType
from synthetic.event.log.general.media import MediaEvent
from synthetic.user.session_engagement_user import SessionEngagementUser


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.profiles = {
        "image_guy": ProfileConfig(
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
            event_probabilities={EventType.IMAGE: 1.0},
            events={EventType.IMAGE: EventConfig(properties={"duration_seconds_min": 60, "duration_seconds_max": 60})},
        )
    }


@pytest.fixture
def registration_ts():
    return datetime(2000, 1, 1, 0, 0, 0)


@pytest.fixture
def image_user(driver_meta, registration_ts):
    return SessionEngagementUser.create_random_user(driver_meta.id, registration_ts, profile_name="image_guy")


def test_single_session_with_images(db_session, registration_ts, image_user):
    random.seed(0)

    CatalogCache.warm_up(db_session=db_session)

    end_ts = registration_ts + timedelta(days=1)
    events = image_user.generate_events(end_ts)
    log_events = events.log_events
    log_events.extend(image_user.get_scheduled_log_events())

    log_events = [event for event in log_events if isinstance(event, MediaEvent)]

    assert len(log_events) == 2

    image_uuid = log_events[0].props["id_source"]
    assert log_events[0].props == {
        "action": "play",
        "id": f"image_{image_uuid}",
        "id_source": image_uuid,
        "type": "image",
        "time": 0,
    }

    assert log_events[1].props == {
        "action": "impression",
        "id": f"image_{image_uuid}",
        "id_source": image_uuid,
        "type": "image",
        "time": 0,
    }
