import pytest

from datetime import datetime
from synthetic.conf import EngagementConfig, NudgeConfig, ProfileConfig, global_conf
from synthetic.event.constants import EventType, NudgeResponseAction, NudgeType
from synthetic.user.session_engagement_user import SessionEngagementUser
from synthetic.utils.nudge_utils import Nudge
from synthetic.utils.test_utils import assert_dicts_equal_partial


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.profiles = {
        "nudge_guy": ProfileConfig(
            session_engagement=EngagementConfig(initial_min=1.0, initial_max=1.0, change_probability=0.0),
            occurrence_probability=1.0,
            session_min_count=1,
            session_max_count=1,
            session_event_type_changes_min=0,
            session_event_type_changes_max=0,
            session_length_min_seconds=1000,
            session_length_max_seconds=1000,
            online_probability=0.5,
            event_probabilities={EventType.MODULE: 1.0},
            nudges=NudgeConfig(
                engagement_effect=EngagementConfig(
                    boost_probability=0.0, decay_probability=1.0, change_probability=1.0, change_min=0.5, change_max=0.5
                )
            ),
        )
    }


@pytest.fixture
def registration_ts():
    return datetime(2000, 1, 1, 0, 0, 0)


@pytest.fixture
def nudge_user(driver_meta, registration_ts) -> SessionEngagementUser:
    return SessionEngagementUser.create_random_user(driver_meta.id, registration_ts, profile_name="nudge_guy")


def test_nudge_response(nudge_user):
    nudge_profile = global_conf.profiles["nudge_guy"]
    nudge_profile.nudges.response_probabilities = {NudgeResponseAction.OPEN: 1.0}
    nudge_profile.nudges.max_bonus_session_count = 0

    pre_engagement = nudge_user.get_session_engagement_manager().get_engagement()
    assert pre_engagement == 1.0

    nudge = Nudge(nudge_id=123, subject_id=nudge_user.get_platform_uuid(), queued_at=datetime(2001, 1, 1, 0, 0, 0))
    response = nudge_user.receive_nudge(nudge, datetime(2001, 1, 1, 0, 5, 0))

    assert_dicts_equal_partial(
        response.as_payload_dict(),
        {
            'block': 'core',
            'props': {
                'nudge_id': 123,
                'response': {'action': NudgeResponseAction.OPEN.value},
                'type': NudgeType.PUSH_NOTIFICATION.value,
            },
            'ts': '2001-01-01T00:05:00.000000Z',
            'type': 'nudge_response',
            'u_id': nudge_user.get_platform_uuid(),
        },
    )

    post_engagement = nudge_user.get_session_engagement_manager().get_engagement()
    assert post_engagement == 0.5
