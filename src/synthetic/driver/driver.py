import os
import dataclasses
import logging
import pickle
import random
import time
import uuid
from collections import defaultdict

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple, Any

from psycopg2 import OperationalError

from synthetic.catalog.generator import create_catalog_event_for_type
from synthetic.constants import CatalogType, SECONDS_IN_DAY
from synthetic.conf import global_conf
from synthetic.database.db_cache import DatabaseCache
from synthetic.catalog.cache import CatalogCache, clean_promo_catalogs
from synthetic.event.event_collection import EventCollection
from synthetic.event.log.commerce.cancel_checkout import CancelCheckoutEvent, CancelType
from synthetic.event.log.commerce.constants import ItemType, ItemObject
from synthetic.event.log.commerce.delivery import DeliveryEvent, DeliveryAction
from synthetic.event.log.generator import generate_rate_events
from synthetic.event.log.loyalty.promo import PromoType
from synthetic.event.meta.meta_base import MetaEvent
from synthetic.event.meta.receive_nudge import ReceiveNudges
from synthetic.sink.factory import build_sink_from_type
from synthetic.sink.flush_sink import FlushSink
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.log_base import LogEvent
from synthetic.managers.managed_object import ManagedObject
from synthetic.managers.population import PopulationManager
from synthetic.sink.memory_flush_sink import MemoryFlushSink
from synthetic.utils.database import create_db_session, get_current_memory_usage_kb, store_catalogs_in_db
from synthetic.user.factory import (
    load_users_from_db,
    store_user_in_db,
    create_random_user,
    load_user_from_db,
)
from synthetic.user.synthetic_user import SyntheticUser, find_first_registered_user
from synthetic.utils.nudge_utils import get_nudges_from_backend
from synthetic.utils.random import (
    select_random_profile_names_based_on_counts,
    select_random_keys_from_dict,
    get_random_float_in_range,
    get_random_int_in_range,
)
from synthetic.utils.slack_notifier import Slack, MessageType
from synthetic.utils.time_utils import total_difference_seconds
from synthetic.utils.current_time_utils import get_current_time
from synthetic.utils.user_utils import fake

logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)

CONTINUOUS_REPORTING = True


@dataclasses.dataclass
class InactiveUser:
    platform_uuid: str
    last_seen_ts: datetime

    @classmethod
    def from_user(cls, user: SyntheticUser) -> "InactiveUser":
        return InactiveUser(platform_uuid=user.get_platform_uuid(), last_seen_ts=user.last_seen_ts)

    def __lt__(self: "InactiveUser", other: "InactiveUser") -> bool:
        return self.platform_uuid < other.platform_uuid

    def __hash__(self):
        return hash(self.platform_uuid)

    @classmethod
    def from_dict(cls, inactive_user_dict: Dict[str, Any]) -> "InactiveUser":
        return InactiveUser(
            inactive_user_dict["platform_uuid"], datetime.fromtimestamp(inactive_user_dict["last_seen_timestamp"])
        )

    def to_dict(self) -> Dict[str, Any]:
        return {"platform_uuid": self.platform_uuid, "last_seen_timestamp": self.last_seen_ts.timestamp()}


class Driver(ManagedObject):
    def __init__(
        self,
        sink_types: Optional[List[str]] = None,
        clear_cache_after_flush: bool = True,
        time_increment_interval_seconds: int = 3600,
    ):
        super().__init__()

        if sink_types is None:
            sink_types = ["memory"]

        self._driver_data: Dict[str, Any] = {}
        self._driver_meta_id: Optional[int] = None

        self._log_sinks: List[FlushSink] = [build_sink_from_type(sink_type) for sink_type in sink_types]

        self._running = True
        self._time_increment_interval_seconds = time_increment_interval_seconds
        self._sleep_interval_seconds = 5
        self._active_users: List[SyntheticUser] = []
        self._inactive_users: List[InactiveUser] = []

        self._first_run = True
        self._reset_population = False

        self._last_seen_ts: Optional[datetime] = None
        self._last_online_persistence_ts: Optional[datetime] = None
        self._last_maintenance_ts: Optional[datetime] = None

        self._clear_cache_after_flush = clear_cache_after_flush
        self._cached_log_events: List[LogEvent] = []
        self._cached_catalog_events: List[CatalogEvent] = []
        self._cached_meta_events: List[MetaEvent] = []

        if global_conf.cache_logs_on_failure:
            self.restore_cache_from_disk()

        self._detached_events = EventCollection()

        self._flushed_log_count = 0
        self._flushed_catalog_count = 0
        self._flushed_meta_count = 0

        population_manager = PopulationManager(self._driver_data, global_conf.population, global_conf.start_ts)
        population_manager.initialize()
        self.add_manager(population_manager)

    def set_time_increment_interval_seconds(self, seconds: int):
        self._time_increment_interval_seconds = seconds

    def get_flush_sinks(self) -> List[FlushSink]:
        return self._log_sinks

    def set_clear_cache_after_flush(self, clear):
        self._clear_cache_after_flush = clear

    def report_flushing(self):
        if self._flushed_log_count + self._flushed_catalog_count > 0:
            logger.info(
                "Recently flushed %s log events and %s catalog events.",
                self._flushed_log_count,
                self._flushed_catalog_count,
            )
            self.clear_counts()

    def get_cached_log_events(self) -> List[LogEvent]:
        return self._cached_log_events

    def get_cached_catalog_events(self) -> List[CatalogEvent]:
        return self._cached_catalog_events

    def get_cached_meta_events(self) -> List[MetaEvent]:
        return self._cached_meta_events

    def get_cached_events(self) -> EventCollection:
        return EventCollection(
            log_events=self._cached_log_events,
            catalog_events=self._cached_catalog_events,
            meta_events=self._cached_meta_events,
        )

    def clear_counts(self):
        self._flushed_log_count = 0
        self._flushed_catalog_count = 0
        self._flushed_meta_count = 0

    def clear_cache(self):
        self._cached_log_events = []
        self._cached_catalog_events = []
        self._cached_meta_events = []

    def set_driver_data_from_db(self, driver_data_from_db: Dict[str, Any]):
        self._driver_data = driver_data_from_db

        if "inactive_users" in driver_data_from_db:
            self._inactive_users = [
                InactiveUser.from_dict(inactive_user_dict)
                for inactive_user_dict in driver_data_from_db["inactive_users"]
            ]
            del driver_data_from_db["inactive_users"]

        self.set_manager_data(self._driver_data)

    def get_driver_data_for_db(self) -> Dict[str, Any]:
        driver_data = self._driver_data.copy()
        driver_data["inactive_users"] = [inactive_user.to_dict() for inactive_user in self._inactive_users]

        return driver_data

    @property
    def last_seen_ts(self):
        return self._last_seen_ts

    @last_seen_ts.setter
    def last_seen_ts(self, last_seen_ts):
        self._last_seen_ts = last_seen_ts

    @property
    def last_maintenance_ts(self):
        return self._last_maintenance_ts

    @last_maintenance_ts.setter
    def last_maintenance_ts(self, last_maintenance_ts):
        self._last_maintenance_ts = last_maintenance_ts

    def _queued_log_events(self, events: List[LogEvent]):
        self._cached_log_events.extend(events)

    def _queued_catalog_events(self, events: List[CatalogEvent]):
        self._cached_catalog_events.extend(events)

    def _queued_meta_events(self, events: List[MetaEvent]):
        self._cached_meta_events.extend(events)

    def queue_events_for_flush(self, events: EventCollection, verification_ts: datetime = None):
        if verification_ts is not None:
            events.assert_integrity(verification_ts)

        self._queued_log_events(events.log_events)
        self._queued_catalog_events(events.catalog_events)
        self._queued_meta_events(events.meta_events)

    @property
    def driver_meta_id(self) -> int:
        assert self._driver_meta_id is not None

        return self._driver_meta_id

    def should_flush(self) -> bool:
        return True

    def _update_from_flushed_log_events(self, log_events: List[LogEvent]):
        for event in log_events:
            event.update_driver_after_flush(self)

    @classmethod
    def get_cache_filename(cls) -> str:
        dirname = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(dirname, f"{global_conf.organisation}_{global_conf.project}.pkl")

    def persist_cache_to_disk(self):
        if len(self._cached_log_events) + len(self._cached_catalog_events) + len(self._cached_meta_events) == 0:
            return

        logger.info("Persisting cached logs to disk...")
        with open(self.get_cache_filename(), "wb") as cache_file:
            pickle.dump(
                {
                    "logs": self._cached_log_events,
                    "catalogs": self._cached_catalog_events,
                    "meta": self._cached_meta_events,
                },
                cache_file,
            )
        logger.info("Persisted cached logs to disk!")

    def restore_cache_from_disk(self):
        filename = self.get_cache_filename()
        if os.path.exists(filename):
            logger.info("Restoring cached logs from disk...")
            with open(filename, "rb") as cache_file:
                data = pickle.load(
                    cache_file,
                )
                self._cached_log_events.extend(data['logs'])
                self._cached_catalog_events.extend(data['catalogs'])
                self._cached_meta_events.extend(data['meta'])

            os.remove(filename)
            logger.info("Restored cached logs from disk!")

    def flush_events(self, current_ts: datetime):
        logger.debug(
            "Flushing with %s meta events, %s logs and %s catalogs...",
            len(self._cached_meta_events),
            len(self._cached_log_events),
            len(self._cached_catalog_events),
        )
        error_encountered = False
        try:
            if len(self._cached_meta_events) > 0:
                meta_count = len(self._cached_meta_events)
                logger.debug("Flushing %s meta_count events...", meta_count)
                meta_events = self._cached_meta_events
                meta_events.sort(key=lambda event: event.ts)

                if self._cached_meta_events[-1].ts > current_ts:
                    Slack.notify_simple(
                        "Future event",
                        "Future event for current time %s: %s"
                        % (
                            current_ts,
                            self._cached_meta_events[-1],
                        ),
                        MessageType.WARNING,
                    )

                for meta_event in meta_events:
                    consequence_events = meta_event.perform_actions()
                    if consequence_events is None:
                        continue

                    for sink in self._log_sinks:
                        sink.flush_log_events(consequence_events.log_events)
                        sink.flush_catalog_events(consequence_events.catalog_events)

            # Manage detached events
            if len(self._cached_log_events) > 0:
                log_events = self._cached_log_events
                self._update_from_flushed_log_events(log_events)
            detached_events = self._generate_detached_events(current_ts)
            if not detached_events.is_empty():
                self.queue_events_for_flush(detached_events, current_ts)

            if len(self._cached_log_events) > 0:
                log_events = self._cached_log_events
                log_events.sort(key=lambda event: event.ts)

                if self._cached_log_events[-1].ts > current_ts:
                    Slack.notify_simple(
                        "Future event",
                        "Future event for current time %s: %s"
                        % (
                            current_ts,
                            self._cached_log_events[-1],
                        ),
                        MessageType.WARNING,
                    )

                log_count = len(self._cached_log_events)
                logger.debug("Flushing %s log events...", log_count)
                for sink in self._log_sinks:
                    sink.flush_log_events(log_events)

                self._flushed_log_count += log_count

            if len(self._cached_catalog_events) > 0:
                catalog_count = len(self._cached_catalog_events)
                logger.debug("Flushing %s catalog events...", catalog_count)
                catalog_events = self._cached_catalog_events
                catalog_events.sort(key=lambda event: event.ts)

                if self._cached_catalog_events[-1].ts > current_ts:
                    Slack.notify_simple(
                        "Future event",
                        "Future event for current time %s: %s"
                        % (
                            current_ts,
                            self._cached_catalog_events[-1],
                        ),
                        MessageType.WARNING,
                    )

                for sink in self._log_sinks:
                    sink.flush_catalog_events(catalog_events)

                self._flushed_catalog_count += catalog_count

            if self._clear_cache_after_flush:
                self._cached_log_events.clear()
                self._cached_catalog_events.clear()
                self._cached_meta_events.clear()
        except Exception:
            error_encountered = True
            raise
        finally:
            if error_encountered and global_conf.cache_logs_on_failure:
                # Write logs to disk
                self.persist_cache_to_disk()

    def initialize_from_db(self) -> Dict[CatalogType, List[CatalogEvent]]:

        with create_db_session() as db_session:
            # First attempt to load metadata
            driver_meta_data = DatabaseCache.get_driver_meta(
                global_conf.organisation, global_conf.project, db_session=db_session
            )
            # driver_meta_data = load_driver_meta_from_db(db_session, global_conf.organisation, global_conf.project)

            if driver_meta_data is None:
                logger.info("First run! Initializing...")
                driver_meta_data = DatabaseCache.store_driver_meta(
                    global_conf.organisation,
                    global_conf.project,
                    {
                        "last_seen_ts": global_conf.start_ts,
                        "last_maintenance_ts": global_conf.start_ts,
                        "driver_data": self.get_driver_data_for_db(),
                    },
                    db_session=db_session,
                )

                self._driver_meta_id = driver_meta_data["id"]

                self._last_seen_ts = global_conf.start_ts
                self._last_maintenance_ts = global_conf.start_ts

                logger.info(
                    "Config Info: Last seen %s, last maintenance %s",
                    self.last_seen_ts,
                    self.last_maintenance_ts,
                )

                self._first_run = True
            else:
                self._driver_meta_id = driver_meta_data["id"]
                self._last_seen_ts = driver_meta_data["last_seen_ts"]
                self._last_maintenance_ts = driver_meta_data["last_maintenance_ts"]
                self.set_driver_data_from_db(driver_meta_data["driver_data"])

                logger.info("Resuming from previous run...")
                logger.info(
                    "DB Info: Last seen %s, last maintenance %s",
                    driver_meta_data["last_seen_ts"],
                    driver_meta_data["last_maintenance_ts"],
                )
                self._first_run = False

            assert self._driver_meta_id is not None

            self._integrate_users(
                active_users=load_users_from_db(db_session, driver_meta_id=self._driver_meta_id, active_only=True)
            )

            if self._reset_population:
                self.get_population_manager().reset()
                for user in self._active_users:
                    user.force_churn()

            new_catalogs = CatalogCache.warm_up(
                db_session, driver_meta_id=driver_meta_data["id"], current_ts=self._last_seen_ts
            )

        if global_conf.use_promotions:
            CatalogCache.update_current_promotions()

        return new_catalogs

    def _integrate_users(self, active_users: List[SyntheticUser]):
        self._active_users = active_users

    def _persist_to_db(self):
        attempt_count = 10
        current_wait_time = 2
        while attempt_count > 0:
            try:
                logger.info("Persisting state to DB...")
                with create_db_session() as db_session:
                    driver_meta_data: Dict[str, Any] = DatabaseCache.get_driver_meta(
                        global_conf.organisation, global_conf.project, db_session=db_session
                    )
                    logger.info("Storing driver meta data...")
                    if driver_meta_data is None:
                        driver_meta_data = {
                            "last_seen_ts": self._last_seen_ts,
                            "last_maintenance_ts": self._last_maintenance_ts,
                            "driver_data": self.get_driver_data_for_db(),
                        }
                    else:
                        driver_meta_data["last_seen_ts"] = self._last_seen_ts
                        driver_meta_data["last_maintenance_ts"] = self._last_maintenance_ts
                        driver_meta_data["driver_data"] = self.get_driver_data_for_db()

                    DatabaseCache.store_driver_meta(
                        global_conf.organisation, global_conf.project, driver_meta_data, db_session=db_session
                    )

                    logger.debug(
                        "Persisted driver state for %s, %s last seen %s, last maintenance %s!",
                        global_conf.organisation,
                        global_conf.project,
                        driver_meta_data["last_seen_ts"],
                        driver_meta_data["last_maintenance_ts"],
                    )

                    logger.info("Storing fresh users...")
                    # Now we store fresh users
                    unstored_user_count = 0
                    for user in self._active_users:
                        store_user_in_db(db_session, driver_meta_data["id"], user)
                        unstored_user_count += 1

                        if unstored_user_count > 100:
                            db_session.commit()
                            unstored_user_count = 0

                    db_session.commit()

                    logger.info("Persistence completed!")
                    logger.critical(
                        "Final user counts: active %s, inactive %s", len(self._active_users), len(self._inactive_users)
                    )
                    self.log_user_profiles()

                    return
            except OperationalError as e:
                logger.error("Could not connect to database: %s", e)
                logger.error("Waiting %s seconds before trying again...", current_wait_time)
                time.sleep(current_wait_time)

                attempt_count -= 1
                current_wait_time *= 2

    def log_user_profiles(self):
        profile_counts = defaultdict(lambda: 0)
        for active_user in self._active_users:
            profile_counts[active_user.profile_name] += 1
        for profile_name in sorted(profile_counts):
            logger.info("* %s: %s" % (profile_name, profile_counts[profile_name]))

    def get_scheduled_order_deliveries(self) -> List[Dict[str, Any]]:
        if "scheduled_deliveries" not in self._driver_data:
            self._driver_data["scheduled_deliveries"] = []

        scheduled_deliveries = self._driver_data["scheduled_deliveries"]
        return scheduled_deliveries

    def get_scheduled_order_cancellations(self) -> List[Dict[str, Any]]:
        if "scheduled_order_cancellations" not in self._driver_data:
            self._driver_data["scheduled_order_cancellations"] = []

        scheduled_order_cancellations = self._driver_data["scheduled_order_cancellations"]
        return scheduled_order_cancellations

    def get_active_users(self) -> List[SyntheticUser]:
        return self._active_users

    def get_inactive_users(self) -> List[InactiveUser]:
        return self._inactive_users

    def get_population_manager(self) -> PopulationManager:
        return self.get_manager("population")

    def _maintain_user_population(self, current_ts: datetime):
        logger.debug(
            "Maintaining active user population on %s between %s and %s (volatility %s)...",
            current_ts,
            global_conf.population.target_min_count,
            global_conf.population.target_max_count,
            global_conf.population.volatility,
        )
        active_user_count = len(self.get_active_users())

        logger.debug("Active users: %s", active_user_count)
        target_active_user_count = self.get_population_manager().get_population()
        if active_user_count < target_active_user_count:
            # We add users to reach the target
            forced_added_user_count = target_active_user_count - active_user_count
            self._add_random_users(current_ts, forced_added_user_count)
            active_user_count += forced_added_user_count
        elif (
            global_conf.population.prune_oldest_registered_users
            and active_user_count > global_conf.population.target_max_count * (1.0 + global_conf.population.volatility)
        ):
            # Churn the oldest user
            oldest_user = find_first_registered_user(self._active_users, current_ts)
            if oldest_user is not None:
                logger.critical(
                    "Churning first registered user %s registered on %s, due to overpopulation...",
                    oldest_user.get_platform_uuid(),
                    oldest_user.registration_ts,
                )
                oldest_user.force_churn()

        logger.debug("Population finalised with %s active users!", active_user_count)

    def _add_random_user(
        self,
        driver_meta_id: int,
        current_ts: datetime,
        platform_uuid: Optional[str],
        profile_name: str = None,
    ):
        assert isinstance(driver_meta_id, int)

        registration_ts = current_ts
        if global_conf.randomise_registration_times:
            registration_ts += timedelta(seconds=random.random() * SECONDS_IN_DAY)

        logger.debug("Creating random user for %s/%s...", platform_uuid, profile_name)
        new_random_user = create_random_user(
            driver_meta_id, registration_ts, platform_uuid=platform_uuid, profile_name=profile_name
        )
        self._active_users.append(new_random_user)

    def _add_random_users(self, current_ts: datetime, user_count: int):
        if user_count == 0:
            return

        logger.info("Adding %s random users on %s...", user_count, current_ts)

        uuids = [None] * user_count
        self._add_random_users_for_uuids(current_ts, uuids)

    def _count_active_profiles(self) -> Dict[str, int]:
        active_profile_counts: Dict[str, int] = {}
        for user in self._active_users:
            profile_name = user.get_profile_data()["profile_name"]
            if profile_name not in active_profile_counts:
                active_profile_counts[profile_name] = 0

            active_profile_counts[profile_name] += 1

        return active_profile_counts

    def _add_random_users_for_uuids(self, current_ts: datetime, uuids: Sequence[Optional[str]]):
        if len(uuids) == 0:
            return

        with create_db_session() as db_session:
            driver_meta_data = DatabaseCache.get_driver_meta(
                global_conf.organisation, global_conf.project, db_session=db_session
            )
            if driver_meta_data is None:
                raise ValueError("No driver meta")

            if global_conf.manage_population_counts_per_profile:
                profile_counts = self._count_active_profiles()
                desired_population_count = self.get_population_manager().get_population()
                self.log_user_profiles()
                profile_names = select_random_profile_names_based_on_counts(
                    desired_population_count, profile_counts, global_conf.profiles, generated_count=len(uuids)
                )
            else:
                profile_names = select_random_keys_from_dict(global_conf.profiles, count=len(uuids))

            for platform_uuid, profile_name in zip(uuids, profile_names):
                self._add_random_user(
                    driver_meta_data["id"], current_ts, platform_uuid=platform_uuid, profile_name=profile_name
                )

    def _initialize_actors(self, current_ts: datetime):
        desired_initial_count = global_conf.population.initial_count
        if desired_initial_count == 0:
            return

        self._add_random_users(current_ts, desired_initial_count)

    def _resurrect_user(self, platform_uuid: str, current_ts: datetime, resurrection_data: Dict[str, Any]):
        logger.info("Resurrecting %s on %s...", platform_uuid, current_ts)
        logger.debug("Memory use at start: %s", get_current_memory_usage_kb())
        assert platform_uuid not in [user.get_platform_uuid() for user in self._active_users]

        with create_db_session() as db_session:
            assert self._driver_meta_id is not None
            logger.debug("Memory use right before loading user: %s", get_current_memory_usage_kb())
            resurrected_user = load_user_from_db(db_session, self._driver_meta_id, platform_uuid)
            logger.debug("Memory use after loading user: %s", get_current_memory_usage_kb())
            events = resurrected_user.prepare_after_resurrection(current_ts, resurrection_data)
            logger.debug("Memory use after preparing user: %s", get_current_memory_usage_kb())
            if events is not None:
                self.queue_events_for_flush(events, current_ts)
                logger.debug("Memory use after caching events: %s", get_current_memory_usage_kb())

            self._active_users.append(resurrected_user)
            logger.debug("Memory use after appending user: %s", get_current_memory_usage_kb())

        logger.debug("Memory use at end: %s", get_current_memory_usage_kb())

    def force_user_to_churn(self, user_uuid: str):
        for user in self._active_users:
            if user.get_platform_uuid() != user_uuid:
                continue

            user.force_churn()
            self._organise_users()
            self._persist_to_db()
            return

        raise ValueError("Active user %s not found!" % (user_uuid,))

    def _check_user_nudge_resurrection(self, inactive_user: InactiveUser) -> Optional[Dict[str, Any]]:
        nudges = get_nudges_from_backend(
            global_conf.api_url,
            global_conf.api_key,
            inactive_user.platform_uuid,
            last_queued_at=inactive_user.last_seen_ts,
        )

        if len(nudges) > 0:
            return {"nudges": nudges}

        return None

    def _check_user_resurrections(
        self, current_ts: datetime, last_update_ts: datetime
    ) -> Tuple[List[InactiveUser], Dict[str, Dict[str, Any]]]:
        current_inactive_users = self._inactive_users
        resurrection_data: Dict[str, Dict[str, Any]] = {}

        if (
            global_conf.use_nudges
            and global_conf.population.inactive_nudge_check_ratio_per_hour > 0.0
            and current_ts > get_current_time() - timedelta(hours=1)
            and len(current_inactive_users) > 0
        ):
            ratio_scaler = float((current_ts - last_update_ts).seconds) / 3600
            actual_check_ratio = global_conf.population.inactive_nudge_check_ratio_per_hour * ratio_scaler

            inactive_user_check_count = max(1, int(round(actual_check_ratio * len(current_inactive_users))))
            inactive_users_checked = random.choices(current_inactive_users, k=inactive_user_check_count)
            logger.info("Checking for nudges on %s inactive users...", len(inactive_users_checked))

            for inactive_user in inactive_users_checked:
                user_resurrection_data = self._check_user_nudge_resurrection(inactive_user)

                if user_resurrection_data is not None:
                    resurrection_data[inactive_user.platform_uuid] = user_resurrection_data

        if len(resurrection_data) > 0:
            logger.info("Resurrecting %s users due to nudging...", len(resurrection_data))

        if (
            len(current_inactive_users) > 0
            and global_conf.population.resurrection_probability > 0.0
            and random.random() < global_conf.population.resurrection_probability
        ):
            # Someone got resurrected
            resurrected_user = random.choice(current_inactive_users)
            resurrection_data[resurrected_user.platform_uuid] = {"engagement_delta": 1.0}
            logger.info("Resurrecting a random user: %s!", resurrected_user.platform_uuid)

        new_inactive_users = [user for user in current_inactive_users if user.platform_uuid not in resurrection_data]

        return new_inactive_users, resurrection_data

    def update_inactive_users(self, current_ts: datetime, last_update_ts: datetime):
        logger.debug("Memory use before updating inactive users: %s", get_current_memory_usage_kb())

        new_inactive_users, resurrections = self._check_user_resurrections(current_ts, last_update_ts)

        logger.debug("Memory use after checking resurrections: %s", get_current_memory_usage_kb())

        for user_id, resurrection_data in resurrections.items():
            self._resurrect_user(user_id, current_ts, resurrection_data)
        self._inactive_users = new_inactive_users

        logger.debug("Memory use after updating inactive users: %s", get_current_memory_usage_kb())

    def prepare_actors_for_event_generation(self, current_ts: datetime):
        assert self.last_maintenance_ts is not None

        logger.debug("Maintaining actors up to %s...", current_ts)
        self._maintain_user_population(current_ts)

        check_timedelta = timedelta(minutes=10)
        if self.last_maintenance_ts <= current_ts - check_timedelta:
            logger.info("Performing regular maintenance...")

            self.update_inactive_users(current_ts, self.last_maintenance_ts)

            self.last_maintenance_ts = current_ts

    def _generate_detached_events(self, end_ts: datetime) -> EventCollection:
        detached_events = self._detached_events.pop_events_before(end_ts)
        detached_events.assert_integrity(end_ts)
        return detached_events

    def get_active_user_with_uuid(self, uuid: str) -> Optional[SyntheticUser]:
        for user in self._active_users:
            if user.get_platform_uuid() == uuid:
                return user

        return None

    def get_inactive_user_with_uuid(self, uuid: str) -> Optional[InactiveUser]:
        for user in self._inactive_users:
            if user.platform_uuid == uuid:
                return user

        return None

    def generate_events(self, current_ts: datetime, online_mode: bool):
        logger.info(
            "Generating events for %s active (%s target) and %s inactive users up to %s...",
            len(self._active_users),
            self.get_population_manager().get_population(),
            len(self._inactive_users),
            current_ts,
        )

        if global_conf.use_promotions:
            self._maintain_promotions(current_ts)
            assert len(CatalogCache.current_promotions) > 0

        for user in self._active_users:
            events = user.generate_events(current_ts, online_mode=online_mode, externally_managed_side_effects=True)

            if not events.is_empty():
                self.queue_events_for_flush(events, current_ts)

        self._cache_order_delivery_events(current_ts)
        self._cache_order_cancellation_events(current_ts)

        logger.info("Generated events!")

    def schedule_detached_events(self, events: EventCollection):
        self._detached_events.insert_events(events)

    def should_report(self, last_reported_ts: datetime, latest_ts: datetime):
        if CONTINUOUS_REPORTING:
            return True
        return last_reported_ts is None or (latest_ts - last_reported_ts).days >= 1

    def _organise_users(self):
        logger.info("Organising users...")
        new_active_users: Dict[str, SyntheticUser] = {}
        recently_inactive_users: Dict[str, SyntheticUser] = {}
        for user in self._active_users:
            if not user.is_active():
                recently_inactive_users[user.get_platform_uuid()] = user
            else:
                new_active_users[user.get_platform_uuid()] = user

        if len(recently_inactive_users) > 0:
            logger.info("%s users just became inactive!", len(recently_inactive_users))

            logger.info("Storing inactive users in db...")
            with create_db_session() as db_session:
                driver_meta_data = DatabaseCache.get_driver_meta(
                    global_conf.organisation, global_conf.project, db_session=db_session
                )
                for user in recently_inactive_users.values():
                    self.schedule_detached_events(user.get_scheduled_events())

                    store_user_in_db(db_session, driver_meta_data["id"], user)
                db_session.commit()

        self._active_users = list(new_active_users.values())
        self._inactive_users = sorted(
            list(
                set(
                    self._inactive_users
                    + [
                        InactiveUser.from_user(recently_inactive_user)
                        for recently_inactive_user in recently_inactive_users.values()
                    ]
                )
            )
        )

    def _wait_and_get_latest_ts(self, online_mode: bool) -> datetime:
        if online_mode:
            latest_ts = get_current_time()
            if self.last_seen_ts >= get_current_time():
                logger.info(
                    "Waiting for time to catch up to experiment last seen %s...",
                    self.last_seen_ts,
                )
                time.sleep(60)
        else:
            latest_ts = self.last_seen_ts + timedelta(seconds=self._time_increment_interval_seconds)
            if global_conf.end_ts is not None:
                # We have to end at the specified dt
                latest_ts = min(latest_ts, global_conf.end_ts)

        return latest_ts

    def _make_report(self, latest_ts: datetime) -> datetime:
        logger.info(
            "%s - %s: Processing from %s to %s with %s out of population target %s active...",
            global_conf.organisation,
            global_conf.project,
            self.last_seen_ts,
            latest_ts,
            len(self._active_users),
            self.get_population_manager().get_population(),
        )

        users_scheduled_to_register = self.get_user_uuids_scheduled_to_register(self.last_seen_ts)
        if len(users_scheduled_to_register) > 0:
            logger.info("%s users scheduled to register!", len(users_scheduled_to_register))

        scheduled_deliveries = self.get_scheduled_order_deliveries()
        if scheduled_deliveries is not None and len(scheduled_deliveries) > 0:
            logger.info("%s deliveries scheduled to be made!", len(scheduled_deliveries))

        last_reported_ts = latest_ts
        return last_reported_ts

    def run(self):
        new_catalogs = self.initialize_from_db()
        if global_conf.notify:
            Slack.notify_simple(
                "Started synthetic data generator for %s_%s" % (global_conf.organisation, global_conf.project),
                message="Resuming from last seen %s..." % (self.last_seen_ts.strftime("%Y-%m-%d %H:%M:%S"),),
                message_type=MessageType.INFO,
            )

        for catalog_type, new_catalog_events_for_type in new_catalogs.items():
            self._queued_catalog_events(new_catalog_events_for_type)

        force_initial_nudge_check = False
        if force_initial_nudge_check:
            receive_nudge_events: List[MetaEvent] = []
            check_ts = get_current_time()
            for user in self._active_users:
                receive_nudge_events.append(ReceiveNudges(user, ts=check_ts))

        if self._first_run:
            self._initialize_actors(self.last_seen_ts)

        logger.info(
            "Starting run from last seen ts %s to %s...",
            self.last_seen_ts,
            global_conf.end_ts,
        )
        self._last_online_persistence_ts = get_current_time()

        last_reported_ts = None
        error_handled = False
        try:
            while self._running and (global_conf.end_ts is None or self.last_seen_ts < global_conf.end_ts):
                logger.debug("Memory usage before daily processing: %s", get_current_memory_usage_kb())
                online_mode = (
                    self.last_seen_ts >= get_current_time()
                    or total_difference_seconds(self.last_seen_ts, get_current_time()) < 3600
                )

                latest_ts = self._wait_and_get_latest_ts(online_mode)

                should_report = self.should_report(last_reported_ts, latest_ts)
                if should_report:
                    last_reported_ts = self._make_report(latest_ts)

                self.prepare_actors_for_event_generation(latest_ts)

                self.generate_events(latest_ts, online_mode=online_mode)

                if self.should_flush():
                    self.flush_events(latest_ts)

                if should_report:
                    self.report_flushing()

                self._organise_users()

                logger.info("Updating driver managers...")
                self.update_managers(latest_ts)

                self.last_seen_ts = latest_ts

                if online_mode:
                    if total_difference_seconds(self._last_online_persistence_ts, get_current_time()) >= 3600:
                        # In online mode, we persist to db occasionally
                        self._persist_to_db()
                        self._last_online_persistence_ts = get_current_time()

                    time.sleep(self._sleep_interval_seconds)

                logger.debug("Memory usage after daily processing: %s", get_current_memory_usage_kb())
        except Exception as e:
            logger.exception(e)
            if global_conf.notify:
                Slack.notify_exception(exc=e, org_proj="%s_%s" % (global_conf.organisation, global_conf.project))
            error_handled = True
            raise e
        finally:
            if not error_handled:
                self.flush_events(current_ts=self.last_seen_ts + timedelta(days=100))
            self._persist_to_db()

        logger.info("All done!")

    def get_user_uuids_scheduled_to_register(self, current_ts) -> List[str]:
        return [user.get_platform_uuid() for user in self._active_users if not user.registered(current_ts)]

    def get_cached_data_as_generated(self) -> Tuple[List[Dict], List[Dict]]:
        log_data = []
        for log_event in self.get_cached_log_events():
            log_data.append(log_event.as_csv_dict())

        catalog_data = []
        for catalog_event in self.get_cached_catalog_events():
            catalog_data.append(catalog_event.as_csv_dict())

        return log_data, catalog_data

    def schedule_delivery(
        self,
        user: SyntheticUser,
        order_id: str,
        order_item_ids: List[str],
        order_item_types: List[ItemType],
        delivery_id: str,
        delivery_ts: datetime,
    ):
        scheduled_deliveries = self.get_scheduled_order_deliveries()

        if order_id in [delivery["order_id"] for delivery in scheduled_deliveries]:
            return

        scheduled_deliveries.append(
            {
                "user_id": user.get_platform_uuid(),
                "user_device_id": user.get_current_device_id(),
                "order_id": order_id,
                "order_item_ids": order_item_ids,
                "order_item_types": [item_type.value for item_type in order_item_types],
                "delivery_id": delivery_id,
                "delivery_timestamp": delivery_ts.timestamp(),
            }
        )

        scheduled_deliveries.sort(key=lambda entry: entry["delivery_timestamp"], reverse=True)

    def _cache_order_delivery_events(self, current_ts: datetime):
        scheduled_deliveries = self.get_scheduled_order_deliveries()
        if len(scheduled_deliveries) > 0:
            delivery_events: List[LogEvent] = []
            rating_events: List[LogEvent] = []
            current_timestamp: float = current_ts.timestamp()
            latest_delivery_timestamp: Optional[float] = scheduled_deliveries[-1].get(
                "delivery_timestamp"
            )  # type: ignore

            while latest_delivery_timestamp is not None and latest_delivery_timestamp <= current_timestamp:
                delivery_data = scheduled_deliveries.pop()
                delivery_user_id = delivery_data["user_id"]
                delivery_user = SyntheticUser(
                    driver_meta_id=None,
                    platform_uuid=delivery_user_id,
                    profile_data={},
                    last_seen_ts=current_ts,
                )
                delivery_user.set_device_id(delivery_data["user_device_id"])
                delivery_ts = datetime.fromtimestamp(delivery_data["delivery_timestamp"])
                delivery_order_id = delivery_data["order_id"]
                delivery_order_item_ids: Optional[List[str]] = (
                    delivery_data["order_item_ids"] if "order_item_ids" in delivery_data else None
                )
                delivery_order_item_catalog_types: Optional[List[str]] = (
                    delivery_data["order_item_types"] if "order_item_types" in delivery_data else None
                )
                delivery_id = delivery_data["delivery_id"]

                delivery_events.append(
                    DeliveryEvent(
                        delivery_user,
                        delivery_ts,
                        order_id=delivery_order_id,
                        delivery_id=delivery_id,
                        action=DeliveryAction.DELIVERED,
                    )
                )

                if len(scheduled_deliveries) > 0:
                    latest_delivery_timestamp = scheduled_deliveries[-1].get("delivery_timestamp")
                else:
                    latest_delivery_timestamp = None

                order_rate_events, _ = generate_rate_events(
                    delivery_user,
                    delivery_ts + timedelta(seconds=random.randint(300, 30000)),
                    delivery_order_id,
                    CatalogType.ORDER,
                )
                if delivery_order_item_ids is not None:
                    assert delivery_order_item_catalog_types is not None
                    for item_id, item_type_str in zip(delivery_order_item_ids, delivery_order_item_catalog_types):
                        item_rate_events, _ = generate_rate_events(
                            delivery_user,
                            delivery_ts + timedelta(seconds=random.randint(300, 30000)),
                            item_id,
                            CatalogType(item_type_str),
                        )
                        order_rate_events.extend(item_rate_events)

                if len(order_rate_events) > 0:
                    rating_events.extend(order_rate_events)

            detached_events = EventCollection(log_events=delivery_events + rating_events)
            self.schedule_detached_events(detached_events)
            if len(delivery_events) > 0:
                logger.info("Made %s deliveries!", len(delivery_events))

            if len(rating_events) > 0:
                logger.info("Made %s order ratings!", len(rating_events))

    def schedule_order_cancellation(
        self,
        cancellation_ts: datetime,
        online: bool,
        user: SyntheticUser,
        order_id: str,
        items: List[ItemObject],
        total_order_price: float,
        reason: str,
    ):
        scheduled_order_cancellations = self.get_scheduled_order_cancellations()

        if order_id in [cancellation["order_id"] for cancellation in scheduled_order_cancellations]:
            return

        scheduled_order_cancellations.append(
            {
                "user_id": user.get_platform_uuid(),
                "user_device_id": user.get_current_device_id(),
                "order_id": order_id,
                "online": online,
                "items": [order_item.get_payload_dict() for order_item in items],
                "total_order_price": total_order_price,
                "reason": reason,
                "cancellation_timestamp": cancellation_ts.timestamp(),
            }
        )

        scheduled_order_cancellations.sort(key=lambda entry: entry["cancellation_timestamp"], reverse=True)

    def _cache_order_cancellation_events(self, current_ts: datetime):
        scheduled_cancellations = self.get_scheduled_order_cancellations()
        if len(scheduled_cancellations) > 0:
            cancellation_events = EventCollection()
            current_timestamp: float = current_ts.timestamp()
            latest_delivery_timestamp: Optional[float] = scheduled_cancellations[-1].get("cancellation_timestamp")

            while latest_delivery_timestamp is not None and latest_delivery_timestamp <= current_timestamp:
                cancellation_event = scheduled_cancellations.pop()
                cancellation_user_id = cancellation_event["user_id"]
                fake_cancellation_user = SyntheticUser(
                    driver_meta_id=None,
                    platform_uuid=cancellation_user_id,
                    profile_data={},
                    last_seen_ts=current_ts,
                )
                fake_cancellation_user.set_device_id(cancellation_event["user_device_id"])
                cancellation_ts = datetime.fromtimestamp(cancellation_event["cancellation_timestamp"])
                cancellation_order_id = cancellation_event["order_id"]
                cancellation_item_data = cancellation_event["items"] if "items" in cancellation_event else []
                cancellation_items: List[ItemObject] = [
                    ItemObject(item["id"], ItemType(item["type"])) for item in cancellation_item_data
                ]
                cancellation_online = cancellation_event["online"]
                cancellation_reason = cancellation_event["reason"]

                cancellation_events.insert_log_event(
                    CancelCheckoutEvent(
                        fake_cancellation_user,
                        cancellation_ts,
                        online=cancellation_online,
                        object_id=cancellation_order_id,
                        cancel_type=CancelType.ORDER,
                        items=cancellation_items,
                        reason=cancellation_reason,
                    )
                )

                real_cancellation_user = self.get_active_user_with_uuid(cancellation_user_id)
                if real_cancellation_user is not None:
                    total_order_price = cancellation_event["total_order_price"]
                    real_profile_data = real_cancellation_user.get_profile_data()
                    account_balance = (
                        real_profile_data["current_account_balance"]
                        if "current_account_balance" in real_profile_data
                        else 0.0
                    )
                    real_cancellation_user.start_event_generation()
                    real_cancellation_user.set_profile_data_value(
                        "current_account_balance", account_balance + total_order_price, change_ts=cancellation_ts
                    )
                    misc_events = real_cancellation_user.finish_event_generation()
                    cancellation_events.insert_events(misc_events)

                if len(scheduled_cancellations) > 0:
                    latest_delivery_timestamp = scheduled_cancellations[-1].get("delivery_timestamp")
                else:
                    latest_delivery_timestamp = None

            if not cancellation_events.is_empty():
                self.queue_events_for_flush(cancellation_events, current_ts)

    def _maintain_promotions(self, current_ts: datetime):
        clean_promo_catalogs(current_ts)
        existing_promotions = CatalogCache.cached_catalog[CatalogType.PROMO]
        catalog_config = global_conf.get_catalog_config(CatalogType.PROMO)
        new_count = catalog_config.target_count - len(existing_promotions)
        if new_count == 0:
            CatalogCache.update_current_promotions()
            return

        new_promotion_catalogs: List[CatalogEvent] = []
        for _ in range(0, new_count):
            min_duration_days = catalog_config.properties.get("length_min_days", 1)
            max_duration_days = catalog_config.properties.get("length_max_days", 31)
            duration_days = (
                random.randrange(min_duration_days, max_duration_days)
                if max_duration_days > max_duration_days
                else max_duration_days
            )
            min_cost_adjustment_ratio = catalog_config.properties.get("cost_adjustment_min_ratio", 0.4)
            max_cost_adjustment_ratio = catalog_config.properties.get("cost_adjustment_max_ratio", 0.95)
            cost_adjustment_ratio = get_random_float_in_range(min_cost_adjustment_ratio, max_cost_adjustment_ratio)
            cost_adjustment_ratio = round(cost_adjustment_ratio * 100.0) / 100.0  # Make it a percentage
            min_item_count = catalog_config.properties.get("item_min_count", 1)
            max_item_count = catalog_config.properties.get("item_max_count", 10)
            item_count = get_random_int_in_range(min_item_count, max_item_count)

            promotion_items: List[Tuple[CatalogType, Dict[str, Any]]] = CatalogCache.get_random_unique_catalogs(
                item_count
            )
            promotion_uuids = [promotion_item[1]["uuid"] for promotion_item in promotion_items]
            promotion_types = [promotion_item[0].value for promotion_item in promotion_items]

            promo_uuid = str(uuid.uuid4())
            promo_data = {
                "uuid": promo_uuid,
                "type": random.choice(list(PromoType)),
                "title": fake.sentence(),
                "cost_adjustment_ratio": cost_adjustment_ratio,
                "promoted_item_uuids": promotion_uuids,
                "promoted_item_types": promotion_types,
                "start_timestamp": current_ts.timestamp(),
                "end_timestamp": (current_ts + timedelta(days=duration_days)).timestamp(),
            }

            new_promotion_catalog = create_catalog_event_for_type(CatalogType.PROMO, current_ts, promo_data)
            CatalogCache.cached_catalog[CatalogType.PROMO][promo_uuid] = promo_data
            new_promotion_catalogs.append(new_promotion_catalog)

        logger.info("Created %s promotions!", len(new_promotion_catalogs))

        self._queued_catalog_events(new_promotion_catalogs)

        with create_db_session(global_conf.db_uri) as db_session:
            assert self._driver_meta_id is not None
            store_catalogs_in_db(db_session, self._driver_meta_id, new_promotion_catalogs)

        CatalogCache.update_current_promotions()

    def get_and_clear_memory_sink_events(self) -> EventCollection:
        sinks = self.get_flush_sinks()
        sink = sinks[0]
        assert isinstance(sink, MemoryFlushSink)
        log_events = sink.flushed_logs[:]
        catalog_events = sink.flushed_catalogs[:]
        sink.clear_all()
        return EventCollection(log_events=log_events, catalog_events=catalog_events)
