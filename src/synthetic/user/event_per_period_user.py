import logging
from datetime import timedelta, datetime
from typing import Dict, List, Optional

from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.event_collection import EventCollection
from synthetic.event.meta.meta_base import MetaEvent
from synthetic.utils.random import select_random_keys_from_dict
from synthetic.utils.time_utils import total_difference_seconds
from synthetic.database.schemas import SyntheticUserSchema
from synthetic.event.log.log_base import LogEvent
from synthetic.event.log.generator import generate_event_logs_of_type
from synthetic.user.constants import SyntheticUserType
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.user_utils import create_user_platform_uuid

logger = logging.getLogger(__name__)


# logger.setLevel(logging.DEBUG)


class EventPerPeriodUser(SyntheticUser):
    """A user that just generates an event every period"""

    @staticmethod
    def create_initial_profile_data(profile_name: str, registration_ts: datetime) -> Dict[str, str]:
        return {
            "profile_name": profile_name,
            "registration_timestamp": str(registration_ts.timestamp()),
        }

    @classmethod
    def create_random_user(
        cls,
        driver_meta_id: int,
        registration_ts: datetime,
        profile_name: str,
        platform_uuid: Optional[str] = None,
    ):
        if platform_uuid is None:
            platform_uuid = create_user_platform_uuid(profile_name)

        user = EventPerPeriodUser(
            driver_meta_id,
            platform_uuid,
            cls.create_initial_profile_data(profile_name, registration_ts=registration_ts),
        )
        return user

    @staticmethod
    def from_db_data(raw_data: SyntheticUserSchema) -> SyntheticUser:
        return EventPerPeriodUser(
            driver_meta_id=raw_data.driver_meta_id,
            platform_uuid=raw_data.platform_uuid,
            profile_data=raw_data.profile_data,
            last_seen_ts=raw_data.last_seen_ts,
        )

    def __init__(
        self,
        driver_meta_id: int,
        platform_uuid: str,
        profile_data: Dict,
        last_seen_ts: Optional[datetime] = None,
    ):
        super(EventPerPeriodUser, self).__init__(driver_meta_id, platform_uuid, profile_data, last_seen_ts=last_seen_ts)
        self._type = SyntheticUserType.EVENT_PER_PERIOD

    def is_active(self):
        return True

    def create_custom_events(self, start_ts: datetime, end_ts: datetime) -> Optional[EventCollection]:
        seconds_per_event = self.get_profile_conf().behaviour.schedule.seconds_per_event
        logger.debug(
            "Creating event_per_period (%s seconds) log events from last seen %s to %s...",
            seconds_per_event,
            start_ts,
            end_ts,
        )

        current_ts = start_ts
        meta_events: List[MetaEvent] = []
        log_events: List[LogEvent] = []
        catalog_events: List[CatalogEvent] = []

        while total_difference_seconds(current_ts, end_ts) >= seconds_per_event:
            logger.debug("Adding event on %s...", current_ts)
            selected_event_type = select_random_keys_from_dict(self._profile_config.event_probabilities, count=1)[0]

            logs_for_type, _ = generate_event_logs_of_type(self, current_ts, selected_event_type, online=True)
            log_events.extend(logs_for_type)

            # We decide how time passes ourselves
            current_ts += timedelta(seconds=seconds_per_event)

        logger.debug("Generated %s events!", len(log_events))

        return EventCollection(meta_events=meta_events, log_events=log_events, catalog_events=catalog_events)
