from uuid import uuid4

import pytest

from synthetic.conf import EngagementConfig, PopulationConfig, ProfileConfig, global_conf
from synthetic.event.constants import EventType
from synthetic.utils.current_time_utils import get_current_time
from synthetic.database.schemas import SyntheticUserSchema
from synthetic.user.session_engagement_user import SessionEngagementUser
from synthetic.user.factory import build_user_from_db_data


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.population = PopulationConfig(initial_count=1, target_max_count=1, target_min_count=1)

    global_conf.profiles = {
        "boring_guy": ProfileConfig(
            occurrence_probability=1.0,
            session_min_count=1,
            session_max_count=1,
            online_probability=0.5,
            session_engagement=EngagementConfig(
                change_probability=0.5,
                change_min=0.05,
                change_max=0.1,
            ),
            event_probabilities={EventType.PAGE: 1.0},
        )
    }


def test_simple_user_persistence(db_session, driver_meta):
    last_seen_ts = get_current_time()

    user = SessionEngagementUser(
        driver_meta_id=driver_meta.id,
        platform_uuid=str(uuid4()),
        profile_data=SessionEngagementUser.create_initial_profile_data("boring_guy", last_seen_ts),
        last_seen_ts=last_seen_ts,
    )

    persisted_user_data = user.get_persisted_user_data()

    user.persist_in_db(db_session, driver_meta.id)

    loaded_db_user = db_session.query(SyntheticUserSchema).filter_by(platform_uuid=user.get_platform_uuid()).one()
    assert loaded_db_user.last_seen_ts == last_seen_ts

    loaded_user = build_user_from_db_data(loaded_db_user)

    assert loaded_user.last_seen_ts == last_seen_ts
    assert loaded_user.get_persisted_user_data() == persisted_user_data
    assert sorted(list(persisted_user_data.keys())) == ['country', 'platform_uuid', 'timezone']
