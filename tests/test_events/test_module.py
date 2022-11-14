import random
import pytest

from datetime import datetime, timedelta
from synthetic.conf import CatalogConfig, EngagementConfig, ProfileConfig, global_conf
from synthetic.catalog.cache import CatalogCache
from synthetic.constants import CatalogType
from synthetic.event.constants import EventType
from synthetic.event.log.learning.module import ModuleEvent
from synthetic.user.session_engagement_user import SessionEngagementUser
from synthetic.user.factory import load_user_from_db
from synthetic.utils.test_utils import assert_dicts_equal_partial, assert_events_have_correct_schema


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.catalogs = {CatalogType.MODULE: CatalogConfig(target_count=1)}

    global_conf.profiles = {
        "module_guy": ProfileConfig(
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
        )
    }


@pytest.fixture
def registration_ts():
    return datetime(2000, 1, 1, 0, 0, 0)


@pytest.fixture
def module_user(driver_meta, registration_ts) -> SessionEngagementUser:
    return SessionEngagementUser.create_random_user(driver_meta.id, registration_ts, profile_name="module_guy")


def test_single_module_completed(db_session, driver_meta, registration_ts, module_user):
    global_conf.catalogs[CatalogType.MODULE] = CatalogConfig(
        target_count=1, properties={"length_min_seconds": 60, "length_max_seconds": 60}
    )

    random.seed(0)

    CatalogCache.warm_up(db_session, driver_meta_id=driver_meta.id)

    end_ts = registration_ts + timedelta(days=1)
    events = module_user.generate_events(end_ts)

    log_events = sorted(events.log_events, key=lambda event: event.ts)
    module_events = sorted(
        [event for event in log_events if isinstance(event, ModuleEvent)], key=lambda event: event.ts
    )

    assert len(module_events) >= 3
    module_uuid = module_events[0].props["id"]

    for index, module_event in enumerate(module_events):
        if index == 0:
            assert_dicts_equal_partial(
                module_event.props,
                {"action": "view", "id": module_uuid, "progress": 0},
            )
        elif index < len(module_events) - 1:
            assert_dicts_equal_partial(
                module_event.props,
                {"action": "view", "id": module_uuid},
            )
            assert 0 <= module_event.props["progress"] < 100
        else:
            assert_dicts_equal_partial(
                module_event.props,
                {"action": "view", "id": module_uuid, "progress": 100},
            )

    milestone_events = [event for event in log_events if event.event_type == "milestone"]
    assert len(milestone_events) > 0
    assert len(module_user.get_profile_data()["milestone_achieved_uuids"]) == len(milestone_events)

    level_events = [event for event in log_events if event.event_type == "level"]
    assert len(level_events) == len(milestone_events)
    for current_level, level_event in enumerate(level_events):
        assert level_event.props["prev_level"] == current_level
        assert level_event.props["new_level"] == current_level + 1

    assert_events_have_correct_schema(events)


def test_single_module_resumed_completed(db_session, driver_meta, registration_ts, module_user):
    global_conf.catalogs[CatalogType.MODULE] = CatalogConfig(
        target_count=1,
        properties={"length_min_seconds": 1600, "length_max_seconds": 1600},
    )

    random.seed(0)

    CatalogCache.warm_up(db_session, driver_meta_id=driver_meta.id)

    first_end_ts = registration_ts + timedelta(days=1)
    events = module_user.generate_events(first_end_ts)
    log_events = events.log_events
    first_module_events = sorted(
        [event for event in log_events if isinstance(event, ModuleEvent)], key=lambda event: event.ts
    )

    assert len(first_module_events) > 0
    module_uuid = first_module_events[0].props["id"]

    for index, module_event in enumerate(first_module_events):
        if index == 0:
            assert module_event.props == {
                "action": "view",
                "id": module_uuid,
                "progress": 0,
            }
        else:
            assert_dicts_equal_partial(
                module_event.props,
                {
                    "action": "view",
                    "id": module_uuid,
                },
            )
            assert 0 <= module_event.props["progress"] < 100

    # Check that we started the module and spent some time on it, but some time remains
    assert module_user.get_profile_data()["active_modules"][module_uuid]["remaining_duration"] > 0

    module_user.persist_in_db(db_session, driver_meta.id)
    module_user = load_user_from_db(db_session, driver_meta.id, module_user.get_platform_uuid())

    second_end_ts = registration_ts + timedelta(days=2)
    events = module_user.generate_events(second_end_ts)
    log_events = events.log_events
    second_module_events = sorted(
        [event for event in log_events if isinstance(event, ModuleEvent)], key=lambda event: event.ts
    )

    # We have spent more than enough time on the module and it should be finished
    assert module_user.get_profile_data()["active_modules"][module_uuid]["remaining_duration"] <= 0

    for index, module_event in enumerate(second_module_events):
        if index < len(second_module_events) - 1:
            assert_dicts_equal_partial(
                module_event.props,
                {"action": "view", "id": module_uuid},
            )
            assert 0 < module_event.props["progress"] < 100
        else:
            assert_dicts_equal_partial(
                module_event.props,
                {"action": "view", "id": module_uuid, "progress": 100},
            )
