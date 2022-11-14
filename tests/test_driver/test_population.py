import pytest

from datetime import datetime, timedelta
from synthetic.conf import (
    EngagementConfig,
    PopulationConfig,
    ProfileConfig,
    global_conf,
)
from synthetic.database.db_cache import DatabaseCache
from synthetic.driver.driver import Driver
from synthetic.event.catalog.user_catalog import UserCatalogEvent
from synthetic.event.constants import EventType
from synthetic.event.log.navigation.identify import IdentifyEvent
from synthetic.utils.database import create_db_session
from synthetic.user.factory import load_users_from_db
from synthetic.utils.test_utils import assert_dicts_equal_partial, assert_events_have_correct_schema


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.population = PopulationConfig(initial_count=25, target_min_count=1, target_max_count=50, volatility=0.3)

    global_conf.profiles = {
        "boring_guy": ProfileConfig(
            occurrence_probability=1.0,
            session_min_count=1,
            session_max_count=1,
            online_probability=0.5,
            session_engagement=EngagementConfig(
                change_probability=1.0,
                change_min=0.1,
                change_max=0.3,
                boost_probability=0.0,
                decay_probability=1.0,
                initial_min=1.0,
                initial_max=1.0,
            ),
            event_probabilities={EventType.PAGE: 1.0},
        )
    }


@pytest.mark.parametrize("manage_population_counts_per_profile", [True, False])
def test_active_population_boundaries(fixed_seed, manage_population_counts_per_profile):
    global_conf.manage_population_counts_per_profile = manage_population_counts_per_profile
    global_conf.start_ts = datetime(2000, 1, 1, 0, 0, 0)
    global_conf.end_ts = datetime(2000, 1, 1, 1, 0, 0)

    driver = Driver(clear_cache_after_flush=True)
    driver.run()

    # We should have initiated all users
    with create_db_session() as db_session:
        all_users = load_users_from_db(db_session=db_session, driver_meta_id=driver.driver_meta_id)
    active_users = [user for user in all_users if user.is_active()]

    assert len(active_users) == global_conf.population.initial_count
    assert len(all_users) == len(active_users)

    current_end_ts = global_conf.start_ts + timedelta(days=1)
    final_end_ts = datetime(2000, 1, 7)

    user_counts = []
    index = 0
    while current_end_ts < final_end_ts:
        global_conf.end_ts = current_end_ts
        driver = Driver(clear_cache_after_flush=True)
        driver.run()

        # Make sure DB is up to date
        with create_db_session() as db_session:
            driver_meta_data = DatabaseCache.get_driver_meta(
                global_conf.organisation, global_conf.project, db_session=db_session
            )
            assert driver_meta_data["last_seen_ts"] == current_end_ts

            all_users = load_users_from_db(db_session=db_session, driver_meta_id=driver_meta_data["id"])
            active_users = [user for user in all_users if user.is_active()]

            user_counts.append(len(active_users))

            assert_dicts_equal_partial(
                driver_meta_data["driver_data"]["managers"]["population"],
                {"last_seen_ts": current_end_ts.timestamp()},
            )

        index += 1
        current_end_ts += timedelta(days=1)

    assert min(user_counts) < global_conf.population.initial_count or global_conf.population.initial_count < max(
        user_counts
    )

    loaded_driver = Driver()
    loaded_driver.initialize_from_db()

    assert sorted([user.get_platform_uuid() for user in loaded_driver.get_active_users()]) == sorted(
        [user.get_platform_uuid() for user in driver.get_active_users()]
    )
    assert sorted([user.platform_uuid for user in loaded_driver.get_inactive_users()]) == sorted(
        [user.platform_uuid for user in driver.get_inactive_users()]
    )


def test_registered_user_maintenance(fixed_seed):
    global_conf.randomise_registration_times = True
    global_conf.population.initial_count = 5
    global_conf.population.target_min_count = 5
    global_conf.population.target_max_count = 5

    global_conf.start_ts = datetime(2000, 1, 1, 0, 0, 0)
    global_conf.end_ts = datetime(2000, 1, 1, 0, 0, 1)

    driver = Driver(clear_cache_after_flush=True)
    driver.run()

    # We should have initiated all users
    with create_db_session() as db_session:
        all_users = load_users_from_db(db_session=db_session, driver_meta_id=driver.driver_meta_id)
        all_user_registrations = [user.get_profile_data()["registration_timestamp"] for user in all_users]
        assert all([not user.registered(global_conf.end_ts) for user in all_users])

        # Check that every user has unique registration time
        assert len(set(all_user_registrations)) == len(all_users)
        assert len(driver.get_active_users()) == len(all_users)

    checked_user_uuids = []
    for hour_offset in range(1, 24):
        global_conf.end_ts = global_conf.start_ts + timedelta(hours=hour_offset)

        driver.run()
        events = driver.get_and_clear_memory_sink_events()

        assert len(driver.get_active_users()) == len(all_users)

        log_logs = events.log_events
        catalog_logs = events.catalog_events

        with create_db_session() as db_session:
            all_users = load_users_from_db(db_session=db_session, driver_meta_id=driver.driver_meta_id)
            for user in all_users:
                if user.get_platform_uuid() in checked_user_uuids:
                    continue

                if user.registered(global_conf.end_ts):
                    identify_logs = [
                        log
                        for log in log_logs
                        if log.user.get_platform_uuid() == user.get_platform_uuid() and isinstance(log, IdentifyEvent)
                    ]
                    assert len(identify_logs) >= 1  # Register and possibly login

                    user_catalogs = [
                        catalog
                        for catalog in catalog_logs
                        if isinstance(catalog, UserCatalogEvent)
                        and catalog.get_platform_uuid() == user.get_platform_uuid()
                    ]
                    assert len(user_catalogs) == 1

                    checked_user_uuids.append(user.get_platform_uuid())

        assert_events_have_correct_schema(events)
