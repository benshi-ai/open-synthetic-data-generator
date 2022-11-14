import pytest
import random

from datetime import datetime

from synthetic.conf import (
    ProfileConfig,
    global_conf,
    EventConfig,
    PopulationConfig,
    BehaviourConfig,
    ScheduleBehaviourConfig,
)
from synthetic.driver.driver import Driver
from synthetic.event.constants import EventType
from synthetic.user.constants import SyntheticUserType


@pytest.fixture(autouse=True)
def fixed_seed():
    random.seed(0)


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.start_ts = datetime(2000, 1, 1, 0, 0, 0)

    global_conf.population = PopulationConfig(initial_count=1, target_max_count=1, target_min_count=1)

    global_conf.profiles = {
        "event_per_second_guy": ProfileConfig(
            user_type=SyntheticUserType.EVENT_PER_PERIOD,
            occurrence_probability=1.0,
            behaviour=BehaviourConfig(schedule=ScheduleBehaviourConfig(seconds_per_event=1)),
            event_probabilities={EventType.PAGE: 1.0},
            events={
                EventType.PAGE: EventConfig(
                    properties={
                        "page_count_per_session_min": 1,
                        "page_count_per_session_max": 1,
                        "duration_seconds_min": 1,
                        "duration_seconds_max": 1,
                    }
                )
            },
        )
    }


def test_offline_batch():
    # Run for two minutes
    global_conf.end_ts = datetime(2000, 1, 1, 0, 2, 1)
    driver = Driver(clear_cache_after_flush=True)
    driver.run()
    assert driver.last_seen_ts == global_conf.end_ts

    events = driver.get_and_clear_memory_sink_events()
    assert len(events.log_events) == 121  # Two minutes, plus register


def test_online_resumed():
    # Run for one minute
    global_conf.end_ts = datetime(2000, 1, 1, 0, 1, 1)
    driver = Driver(clear_cache_after_flush=True)
    driver.run()

    active_users = driver.get_active_users()
    assert len(active_users) == 1
    assert active_users[0].last_seen_ts == global_conf.end_ts

    assert driver.last_seen_ts == global_conf.end_ts

    events = driver.get_and_clear_memory_sink_events()
    assert len(events.log_events) == 61  # One minute, plus register

    global_conf.end_ts = datetime(2000, 1, 1, 0, 2, 2)
    driver = Driver(clear_cache_after_flush=True)
    driver.run()

    active_users = driver.get_active_users()
    assert len(active_users) == 1
    assert active_users[0].last_seen_ts == global_conf.end_ts

    events = driver.get_and_clear_memory_sink_events()
    assert len(events.log_events) == 60  # Resumed one minute, no register
