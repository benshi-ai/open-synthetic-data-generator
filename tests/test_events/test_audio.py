import random
import pytest

from datetime import datetime, timedelta

from synthetic.catalog.cache import CatalogCache
from synthetic.constants import CatalogType
from synthetic.conf import (
    global_conf,
    ProfileConfig,
    EventConfig,
    CatalogConfig,
    EngagementConfig,
)
from synthetic.event.constants import EventType
from synthetic.event.log.general.media import MediaEvent
from synthetic.user.session_engagement_user import SessionEngagementUser
from synthetic.utils.test_utils import assert_dicts_equal_partial, assert_events_have_correct_schema


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.profiles = {
        "audio_guy": ProfileConfig(
            session_engagement=EngagementConfig(initial_min=1.0, initial_max=1.0),
            occurrence_probability=1.0,
            session_min_count=1,
            session_max_count=1,
            session_event_type_changes_min=0,
            session_event_type_changes_max=0,
            session_length_min_seconds=60,
            session_length_max_seconds=60,
            online_probability=0.5,
            event_probabilities={EventType.AUDIO: 1.0},
            events={
                EventType.AUDIO: EventConfig(
                    properties={"pause_probability": 1.0, "duration_seconds_min": 60, "duration_seconds_max": 60}
                )
            },
        )
    }


@pytest.fixture
def registration_ts():
    return datetime(2000, 1, 1, 0, 0, 0)


@pytest.fixture
def audio_user(driver_meta, registration_ts):
    return SessionEngagementUser.create_random_user(driver_meta.id, registration_ts, profile_name="audio_guy")


def test_single_session_with_single_audio(db_session, registration_ts, audio_user):
    global_conf.catalogs = {
        CatalogType.MEDIA_AUDIO: CatalogConfig(
            target_count=1,
            properties={
                "length_min_seconds": 3000,
                "length_max_seconds": 3000,
            },
        )
    }

    random.seed(0)

    CatalogCache.warm_up(db_session=db_session)

    end_ts = registration_ts + timedelta(days=1)
    events = audio_user.generate_events(end_ts)
    log_events = events.log_events
    log_events.extend(audio_user.get_scheduled_log_events())
    assert len(log_events) >= 7

    audio_events = sorted([event for event in log_events if isinstance(event, MediaEvent)], key=lambda event: event.ts)
    assert len(audio_events) == 4

    audio_uuid = audio_events[0].props["id"]
    assert_dicts_equal_partial(
        audio_events[0].props,
        {"action": "impression", "id": audio_uuid, "time": 0, "type": "audio"},
    )

    assert_dicts_equal_partial(
        audio_events[1].props,
        {"action": "play", "id": audio_uuid, "time": 0, "type": "audio"},
    )

    assert_dicts_equal_partial(audio_events[2].props, {"action": "pause", "id": audio_uuid, "type": "audio"})
    assert audio_events[2].props["time"] > 0

    assert_dicts_equal_partial(audio_events[3].props, {"action": "finish", "id": audio_uuid, "type": "audio"})
    assert audio_events[3].props["time"] > audio_events[2].props["time"]

    assert_events_have_correct_schema(events)
