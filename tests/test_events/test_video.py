import random
import pytest

from datetime import datetime, timedelta

from synthetic.catalog.cache import CatalogCache
from synthetic.conf import global_conf, ProfileConfig, EventConfig, EngagementConfig
from synthetic.event.constants import EventType
from synthetic.event.log.general.media import MediaEvent
from synthetic.user.session_engagement_user import SessionEngagementUser
from synthetic.utils.test_utils import assert_dicts_equal_partial, assert_events_have_correct_schema


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.profiles = {
        "video_guy": ProfileConfig(
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
            event_probabilities={EventType.VIDEO: 1.0},
            events={
                EventType.VIDEO: EventConfig(
                    properties={"pause_probability": 1.0, "duration_seconds_min": 60, "duration_seconds_max": 60}
                )
            },
        )
    }


@pytest.fixture
def registration_ts():
    return datetime(2000, 1, 1, 0, 0, 0)


@pytest.fixture
def video_user(driver_meta, registration_ts):
    return SessionEngagementUser.create_random_user(driver_meta.id, registration_ts, profile_name="video_guy")


def test_single_session_with_single_video(db_session, registration_ts, video_user):
    global_conf.rating_probability = 1.0

    random.seed(0)

    CatalogCache.warm_up(db_session=db_session)

    end_ts = registration_ts + timedelta(days=1)
    events = video_user.generate_events(end_ts)
    log_events = events.log_events
    log_events.extend(video_user.get_scheduled_log_events())
    log_events = sorted(log_events, key=lambda event: event.ts)

    video_events = [event for event in log_events if isinstance(event, MediaEvent)]

    assert len(video_events) == 4

    video_uuid = video_events[0].props["id"]
    assert_dicts_equal_partial(
        video_events[0].props,
        {"id": video_uuid, "action": "impression", "time": 0, "type": "video"},
    )

    assert_dicts_equal_partial(
        video_events[1].props,
        {"id": video_uuid, "action": "play", "time": 0, "type": "video"},
    )

    assert_dicts_equal_partial(video_events[2].props, {"id": video_uuid, "action": "pause", "type": "video"})

    assert_dicts_equal_partial(video_events[3].props, {"id": video_uuid, "action": "finish", "type": "video"})

    assert_events_have_correct_schema(events)
