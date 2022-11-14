import logging
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

from synthetic.conf import global_conf, ProfileConfig
from synthetic.constants import BlockType
from synthetic.database.db_session_wrapper import DBSessionWrapper
from synthetic.database.schemas import SyntheticUserSchema
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.catalog.user_catalog import UserCatalogEvent
from synthetic.event.event_collection import EventCollection
from synthetic.event.log.log_base import LogEvent
from synthetic.event.meta.profile_data_update_event import ProfileDataUpdateEvent
from synthetic.managers.managed_object import ManagedObject
from synthetic.user.constants import SyntheticUserType
from synthetic.user.profile_data_update import ProfileDataUpdate, set_variable_in_path
from synthetic.utils.nudge_utils import Nudge, get_nudges_from_backend
from synthetic.utils.current_time_utils import get_current_time
from synthetic.utils.user_utils import generate_random_user_data

logger = logging.getLogger(__name__)

# logger.setLevel(logging.DEBUG)

LOGGED_USER_DATA_NAMES = [
    'country',
    'region_state',
    'city',
    'workplace',
    'timezone',
    'profession',
    'zipcode',
    'language',
    'experience',
    'education_level',
]


class SyntheticUser(ManagedObject):
    """A generic framework for implementing the behaviour of a user"""

    def __init__(
        self,
        driver_meta_id: Optional[int],
        platform_uuid: str,
        profile_data: Dict,
        last_seen_ts: Optional[datetime] = None,
    ):
        super().__init__()

        assert isinstance(platform_uuid, str)

        self._driver_meta_id = driver_meta_id
        self._type: Optional[SyntheticUserType] = None
        self._last_seen_ts: datetime = (
            last_seen_ts
            if last_seen_ts is not None
            else datetime.fromtimestamp(float(profile_data["registration_timestamp"]))
        )
        self._schedule_end_ts = self._last_seen_ts
        self._platform_uuid = platform_uuid
        self._currently_generating_events: Optional[EventCollection] = None

        self._user_data: Optional[Dict[str, str]] = None
        self._profile_data: Dict = profile_data  # Behaviour data about the user

        # Required input profile data
        if "profile_name" in profile_data:
            profile_name = profile_data["profile_name"]
            if (
                global_conf.profiles is None
                or len(global_conf.profiles) == 0
                or profile_name not in global_conf.profiles
            ):
                raise ValueError("Profile %s not configured!" % (profile_name,))

            self._profile_config = global_conf.profiles[profile_name]

        self._scheduled_events = EventCollection()
        self._forced_device_id: Optional[str] = None

    def start_event_generation(self):
        assert self._currently_generating_events is None
        self._currently_generating_events = EventCollection()

    def set_profile_data_value(self, key: str, value: Any, change_ts: Optional[datetime] = None):
        set_variable_in_path(self._profile_data, key, value)

        if change_ts is not None:
            assert self._currently_generating_events is not None
            self._currently_generating_events.insert_meta_event(
                ProfileDataUpdateEvent(self, change_ts, ProfileDataUpdate.create_variable_set_update(key, value))
            )

    def finish_event_generation(self) -> EventCollection:
        assert self._currently_generating_events is not None
        result = self._currently_generating_events
        self._currently_generating_events = None
        return result

    def _update_profile_data_with_user_data(self):
        user_data = self.get_all_user_data()
        self.set_profile_data_value("user_data", {"country": user_data["country"], "timezone": user_data["timezone"]})

    @property
    def last_seen_ts(self):
        return self._last_seen_ts

    @property
    def registration_ts(self) -> datetime:
        registration_timestamp = self._profile_data["registration_timestamp"]
        return datetime.fromtimestamp(float(registration_timestamp))

    @property
    def profile_name(self) -> str:
        return self._profile_data["profile_name"]

    def registered(self, current_ts) -> bool:
        return self.registration_ts < current_ts

    def get_type(self):
        return self._type

    def force_churn(self):
        pass

    def get_profile_conf(self) -> ProfileConfig:
        profile_name = self._profile_data["profile_name"]
        return global_conf.profiles[profile_name]

    def create_events(self, start_ts: datetime, end_ts: datetime) -> Optional[EventCollection]:
        start_ts, events = self.check_registration(start_ts, end_ts)

        if not self.registered(end_ts):
            return None

        custom_events = self.create_custom_events(start_ts, end_ts)
        if custom_events is not None:
            events.insert_events(custom_events)

        logger.debug("Created %s log events and %s catalog events!", len(events.log_events), len(events.catalog_events))
        return events

    def create_custom_events(self, start_ts: datetime, end_ts: datetime) -> Optional[EventCollection]:
        raise NotImplementedError()

    def check_registration(self, start_ts: datetime, end_ts: datetime) -> Tuple[datetime, EventCollection]:
        profile_data = self.get_profile_data()
        registration_ts = datetime.fromtimestamp(float(profile_data["registration_timestamp"]))

        log_events: List[LogEvent] = []
        catalog_events: List[CatalogEvent] = []

        if not self.registered(start_ts) and registration_ts <= end_ts:
            # We haven't registered, yet, now's our chance!
            catalog_events.append(UserCatalogEvent(registration_ts, data=self.get_all_user_data()))

            from synthetic.event.log.navigation.identify import (
                IdentifyEvent,
                IdentifyAction,
            )

            log_events.append(
                IdentifyEvent(
                    self,
                    registration_ts,
                    online=True,
                    action=IdentifyAction.REGISTER,
                )
            )

            logger.debug("Registered user %s on %s!", self.get_platform_uuid(), registration_ts)

            start_ts = registration_ts

        return start_ts, EventCollection(log_events=log_events, catalog_events=catalog_events)

    def generate_events(
        self, end_ts: datetime, online_mode: Optional[bool] = False, externally_managed_side_effects: bool = False
    ) -> EventCollection:
        logger.debug("Generating events from %s to %s...", self._last_seen_ts, end_ts)

        if self._last_seen_ts is None:
            self._last_seen_ts = global_conf.start_ts

        generated_events = EventCollection()

        profile_data = None
        if externally_managed_side_effects:
            profile_data = deepcopy(self._profile_data)

        while self._schedule_end_ts < end_ts:
            generated_events.insert_events(self._scheduled_events)

            self._scheduled_events.clear()
            self._schedule_end_ts = self.fill_event_schedule()

        if externally_managed_side_effects:
            assert profile_data is not None
            self.set_profile_data(profile_data)

        logger.debug(
            "Reading events from schedule from %s to %s, has scheduled (log %s, catalog %s)...",
            self._last_seen_ts,
            end_ts,
            len(self._scheduled_events.log_events),
            len(self._scheduled_events.catalog_events),
        )

        generated_events.insert_events(self._scheduled_events.pop_events_before(end_ts))

        self._last_seen_ts = end_ts

        logger.debug("%s events read!", len(generated_events.log_events))

        for event in generated_events.log_events:
            catalog_events: List[CatalogEvent] = event.generate_associated_catalog_events()
            if len(catalog_events) == 0:
                continue

            generated_events.catalog_events.extend(catalog_events)

        return generated_events

    def set_last_seen_ts(self, last_seen_ts: datetime):
        self._last_seen_ts = last_seen_ts
        self._schedule_end_ts = last_seen_ts

    def fill_event_schedule(self) -> datetime:
        """This should not touch the variable managers, otherwise you get inconsistencies.

        :param db_session:
        :return:
        """
        schedule_duration_seconds: int = 86400

        new_schedule_end_ts = self._schedule_end_ts + timedelta(seconds=schedule_duration_seconds)
        logger.debug(
            "Filling event schedule between %s and %s...",
            self._schedule_end_ts,
            new_schedule_end_ts,
        )

        upcoming_events = self.create_events(self._schedule_end_ts, new_schedule_end_ts)

        if upcoming_events is not None:
            assert isinstance(upcoming_events, EventCollection), upcoming_events
            self._scheduled_events.insert_events(upcoming_events)

            logger.debug(
                "Schedule generated with %s events up to %s!",
                len(self._scheduled_events.log_events),
                new_schedule_end_ts,
            )

            if len(self._scheduled_events.log_events) > 0:
                logger.debug(
                    "Filled log schedule between %s and %s...",
                    self._scheduled_events.log_events[-1].ts,
                    self._scheduled_events.log_events[0].ts,
                )
                assert self._scheduled_events.log_events[0].ts >= self._scheduled_events.log_events[-1].ts
            if len(self._scheduled_events.catalog_events) > 0:
                logger.debug(
                    "Filled log schedule between %s and %s...",
                    self._scheduled_events.catalog_events[-1].ts,
                    self._scheduled_events.catalog_events[0].ts,
                )
                assert self._scheduled_events.catalog_events[0].ts >= self._scheduled_events.catalog_events[-1].ts

        updates = self.update_managers(new_schedule_end_ts)
        for ts, update in updates.items():
            self._scheduled_events.insert_meta_event(ProfileDataUpdateEvent(self, ts, update))

        max_scheduled_event_ts = self._scheduled_events.get_latest_ts()
        if max_scheduled_event_ts is not None:
            new_schedule_end_ts = max_scheduled_event_ts

        return new_schedule_end_ts

    def get_schedule_end_ts(self) -> datetime:
        return self._schedule_end_ts

    def get_scheduled_events(self) -> EventCollection:
        return self._scheduled_events

    def get_scheduled_log_events(self) -> List[LogEvent]:
        return self._scheduled_events.log_events

    def get_all_user_data(self) -> Dict[str, str]:
        if self._user_data is None:
            self._user_data = generate_random_user_data(platform_uuid=self.get_platform_uuid())
            if "user_data" in self._profile_data:
                self._user_data.update(self.get_persisted_user_data())
            else:
                self._update_profile_data_with_user_data()

        return self._user_data

    def get_persisted_user_data(self) -> Dict[str, str]:
        if "user_data" not in self._profile_data:
            logger.warning("Could not find persisted data! Creating random data...")
            self._update_profile_data_with_user_data()

        persisted_user_data = self._profile_data["user_data"].copy()
        persisted_user_data["platform_uuid"] = self.get_platform_uuid()
        return persisted_user_data

    def get_logged_user_data(self) -> Dict[str, str]:
        all_user_data = self.get_all_user_data()

        logged_user_data = dict([(name, all_user_data[name]) for name in LOGGED_USER_DATA_NAMES])
        return logged_user_data

    def get_registration_payload(self) -> Dict[str, str]:
        user_data = self.get_all_user_data()

        return {
            "action": "register",
            "id": self._platform_uuid,
            "country": user_data["country"],
            "region_state": user_data["region_state"],
            "city": user_data["city"],
            "timezone": user_data["timezone"],
            "profession": user_data["profession"],
            "workplace": user_data["workplace"],
        }

    def get_driver_meta_id(self) -> int:
        assert self._driver_meta_id is not None

        return self._driver_meta_id

    def get_platform_uuid(self):
        return self._platform_uuid

    def is_active(self) -> bool:
        raise NotImplementedError()

    def get_profile_data(self) -> Dict:
        return self._profile_data

    def set_profile_data(self, profile_data: Dict):
        self._profile_data = profile_data
        self.set_manager_data(profile_data)

    def start_module(self, module_uuid: str, total_duration: int):
        self.set_profile_data_value(f"active_modules/{module_uuid}/remaining_duration", total_duration)

    def get_level_score(self, block: BlockType) -> float:
        if "level_score" not in self._profile_data:
            self.set_profile_data_value(f"level_score/{block.value}", 0)

        return self._profile_data["level_score"][block.value]

    def set_level_score(self, block: BlockType, score: float, current_ts: datetime):
        self.set_profile_data_value(f"level_score/{block.value}", score, change_ts=current_ts)

    @property
    def milestone_achieved_uuids(self) -> List[str]:
        if "milestone_achieved_uuids" not in self._profile_data:
            self.set_profile_data_value("milestone_achieved_uuids", [])

        return self._profile_data["milestone_achieved_uuids"]

    def set_milestone_achieved(self, milestone_uuid: str, current_ts: datetime):
        new_profile_uuids = (
            self._profile_data["milestone_achieved_uuids"] if "milestone_achieved_uuids" in self._profile_data else []
        )
        new_profile_uuids.append(milestone_uuid)
        self.set_profile_data_value("milestone_achieved_uuids", new_profile_uuids, change_ts=current_ts)

    @property
    def level(self) -> int:
        if "current_level" not in self._profile_data:
            self.set_profile_data_value("current_level", 0)

        return self._profile_data["current_level"]

    def set_current_level(self, level: int, current_ts: datetime):
        self.set_profile_data_value("current_level", level, change_ts=current_ts)

    def progress_module(self, module_uuid: str, duration_seconds: int) -> bool:
        self._profile_data["active_modules"][module_uuid]["remaining_duration"] -= duration_seconds

        if self._profile_data["active_modules"][module_uuid]["remaining_duration"] <= 0:
            return True
        else:
            return False

    def get_module_remaining_duration(self, module_uuid: str) -> int:
        return self._profile_data["active_modules"][module_uuid]["remaining_duration"]

    def get_active_module_uuids(self) -> List[str]:
        if "active_modules" not in self._profile_data:
            return []

        for module_uuid in self._profile_data["active_modules"].copy():
            if self._profile_data["active_modules"][module_uuid]["remaining_duration"] < 0:
                del self._profile_data["active_modules"][module_uuid]

        return [key for key in self._profile_data["active_modules"]]

    def get_current_device_id(self):
        if self._forced_device_id is not None:
            return self._forced_device_id
        return str(hash(self.get_platform_uuid()))

    def persist_in_db(self, db_session: DBSessionWrapper, driver_meta_id: int):
        assert isinstance(driver_meta_id, int)
        db_user = SyntheticUserSchema.create_user_from_data(driver_meta_id, self)
        db_session.add(db_user)
        db_session.commit()

        return db_user

    def get_last_received_nudge_ts(self) -> Optional[datetime]:
        if "last_received_nudge_timestamp" not in self._profile_data:
            return None

        return datetime.fromtimestamp(float(self._profile_data["last_received_nudge_timestamp"]))

    def set_last_received_nudge_ts(self, ts: datetime, current_ts: datetime):
        current_last_ts: Optional[float] = self._profile_data.get("last_received_nudge_timestamp", None)
        new_last_ts = max(current_last_ts, ts.timestamp()) if current_last_ts is not None else ts.timestamp()
        self.set_profile_data_value("last_received_nudge_timestamp", new_last_ts, change_ts=current_ts)

    def receive_nudge(self, nudge: Nudge, received_ts: datetime) -> Optional[LogEvent]:
        logger.debug("Synthetic user ignoring nudge: %s", nudge)
        return None

    def prepare_after_resurrection(
        self, current_ts: datetime, resurrection_data: Dict[str, Any]
    ) -> Optional[EventCollection]:
        pass

    def retrieve_and_receive_nudges(self, received_ts: datetime) -> EventCollection:
        last_nudge_ts = self.get_last_received_nudge_ts()
        last_dispatched_at = get_current_time() - timedelta(hours=4)
        if last_nudge_ts is not None:
            last_dispatched_at = max(last_nudge_ts, last_dispatched_at)

        logger.debug(
            "Retrieving nudges from backend for active user %s, since last nudge %s...",
            self.get_platform_uuid(),
            last_nudge_ts,
        )
        nudges = get_nudges_from_backend(
            global_conf.api_url, global_conf.api_key, self.get_platform_uuid(), last_queued_at=last_dispatched_at
        )

        if len(nudges) > 0:
            logger.debug("Retrieved %s nudges for %s from backend...", len(nudges), self.get_platform_uuid())

        nudge_responses = EventCollection()
        for nudge in nudges:
            if nudge.subject_id != self.get_platform_uuid():
                logger.critical("%s received nudge that was for %s!", self.get_platform_uuid(), nudge.subject_id)
                continue

            if nudge.queued_at > self._last_seen_ts:
                logger.debug("Ignoring nudge from the future!")
                continue
            if last_nudge_ts is not None and nudge.queued_at <= last_nudge_ts:
                logger.debug("Ignoring an old nudge!")
                continue

            nudge_response = self.receive_nudge(nudge, received_ts=received_ts)
            if nudge_response is not None:
                nudge_responses.insert_log_event(nudge_response)

            self.start_event_generation()
            self.set_last_received_nudge_ts(nudge.queued_at, current_ts=received_ts)
            nudge_responses.insert_events(self.finish_event_generation())

        if len(nudge_responses.log_events) > 0:
            self.clear_scheduled_events(received_ts)

        return nudge_responses

    def schedule_events(self, events: EventCollection):
        self._scheduled_events.insert_events(events)

    def clear_scheduled_events(self, clear_ts: datetime):
        self._scheduled_events.clear()
        self._schedule_end_ts = clear_ts

    def set_device_id(self, device_id: str):
        self._forced_device_id = device_id


def find_first_registered_user(
    users: List[SyntheticUser], current_ts: datetime, ignore_recently_nudged_users: bool = True
) -> Optional[SyntheticUser]:
    if len(users) == 0:
        return None

    first_registered_user = None
    for user in users:
        last_nudge_received_ts = user.get_last_received_nudge_ts()
        if (
            ignore_recently_nudged_users
            and last_nudge_received_ts is not None
            and last_nudge_received_ts > current_ts - timedelta(days=7)
        ):
            continue

        if first_registered_user is None or user.registration_ts < first_registered_user.registration_ts:
            first_registered_user = user

    return first_registered_user
