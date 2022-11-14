import logging
import math
import random
from datetime import timedelta, datetime
from typing import List, Dict, Optional, Tuple, Any

from synthetic.conf import global_conf
from synthetic.constants import ProductUserType, CatalogType
from synthetic.event.constants import NudgeResponseAction
from synthetic.event.event_collection import EventCollection
from synthetic.event.log.navigation.app import AppEvent, AppAction
from synthetic.event.log.nudge.nudge_response import NudgeResponseEvent
from synthetic.event.meta.meta_base import MetaEvent
from synthetic.event.meta.receive_nudge import ReceiveNudges
from synthetic.managers.engagement import EngagementManager
from synthetic.utils.event_utils import generate_engagement_delta, calculate_bonus_session_count
from synthetic.utils.nudge_utils import Nudge, generate_random_nudge
from synthetic.utils.random import select_random_keys_from_dict, get_random_float_in_range, get_random_int_in_range
from synthetic.database.schemas import SyntheticUserSchema
from synthetic.event.log.log_base import LogEvent
from synthetic.event.log.navigation.identify import IdentifyEvent, IdentifyAction
from synthetic.event.log.generator import generate_event_logs_of_type, generate_rate_events
from synthetic.user.constants import SyntheticUserType
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.time_utils import total_difference_seconds
from synthetic.utils.user_utils import create_user_platform_uuid

logger = logging.getLogger(__name__)

ENGAGEMENT_SCALED_SESSIONS = False
GUARANTEED_ENGAGEMENT_FOR_SESSION = 0.8


# logger.setLevel(logging.DEBUG)


def enrich_session_events_with_backgrounding(
    session_events: List[LogEvent], online: bool, background_per_minute_probability: float = 0.05
) -> List[LogEvent]:
    if len(session_events) < 2:
        return session_events

    last_session_event = session_events[0]
    new_session_events = [last_session_event]
    last_check_ts = last_session_event.ts

    for session_event in session_events[1:]:
        total_seconds_since_last_check = total_difference_seconds(last_check_ts, session_event.ts)
        if total_seconds_since_last_check >= 60:
            minute_count = math.floor(total_seconds_since_last_check / 60)
            background_count = sum(
                [random.random() < background_per_minute_probability for _ in range(0, minute_count)]
            )
            if background_count > 0:
                background_ts_list = [
                    last_check_ts + timedelta(seconds=random.random() * total_seconds_since_last_check)
                    for _ in range(0, background_count)
                ]
                for background_ts in background_ts_list:
                    new_session_events.append(
                        AppEvent(last_session_event.user, background_ts, online, AppAction.BACKGROUND)
                    )
                    new_session_events.append(
                        AppEvent(
                            last_session_event.user, background_ts + timedelta(seconds=1), online, AppAction.RESUME
                        )
                    )
                    new_session_events.append(
                        IdentifyEvent(
                            last_session_event.user, background_ts + timedelta(seconds=2), online, IdentifyAction.LOGIN
                        )
                    )

            last_check_ts = session_event.ts

        new_session_events.append(session_event)
        last_session_event = session_event

    return new_session_events


class SessionEngagementUser(SyntheticUser):
    """A general user behaviour based on the idea of starting sessions based on a hidden engagement variable."""

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
        assert isinstance(driver_meta_id, int)

        if platform_uuid is None:
            platform_uuid = create_user_platform_uuid(profile_name)

        user = SessionEngagementUser(
            driver_meta_id,
            platform_uuid,
            cls.create_initial_profile_data(profile_name, registration_ts=registration_ts),
        )
        return user

    @staticmethod
    def from_db_data(raw_data: SyntheticUserSchema) -> SyntheticUser:
        return SessionEngagementUser(
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
        super(SessionEngagementUser, self).__init__(
            driver_meta_id, platform_uuid, profile_data, last_seen_ts=last_seen_ts
        )
        self._type = SyntheticUserType.SESSION_ENGAGEMENT

        assert isinstance(driver_meta_id, int)

        profile_conf = self.get_profile_conf()
        session_engagement_config = profile_conf.get_engagement_config("session_engagement")
        session_engagement_manager = EngagementManager(
            profile_data, session_engagement_config, self.registration_ts, "session_engagement"
        )
        session_engagement_manager.initialize()
        self.add_manager(session_engagement_manager)

        self._product_user_type = profile_conf.product_user_type

    @property
    def session_engagement_level(self) -> float:
        if self.get_session_engagement_manager() is None:
            return 0.0

        return self.get_session_engagement_manager().get_engagement()

    def _generate_session_count_for_day(self, session_range_start_ts: datetime) -> int:
        engagement_session_count_factor = self._profile_config.session_engagement_count_factor
        engagement_scaler = (
            (1.0 - engagement_session_count_factor + self.session_engagement_level * engagement_session_count_factor)
            if ENGAGEMENT_SCALED_SESSIONS
            else 1.0
        )
        session_count = round(
            self._profile_config.session_min_count
            + (self._profile_config.session_max_count - self._profile_config.session_min_count)
            * random.random()
            * engagement_scaler
        )

        if global_conf.use_nudges:
            last_received_nudge_ts = self.get_last_received_nudge_ts()
            if last_received_nudge_ts is not None:
                bonus_session_count = calculate_bonus_session_count(
                    last_received_nudge_ts,
                    session_range_start_ts,
                    bonus_session_count=self._profile_config.nudges.bonus_session_count,
                    bonus_session_days=self._profile_config.nudges.bonus_session_days,
                )
                if bonus_session_count > 0:
                    logger.info(
                        "Adding %s bonus sessions to %s for being nudged recently on %s!",
                        bonus_session_count,
                        self._platform_uuid,
                        last_received_nudge_ts,
                    )
                    session_count += bonus_session_count

        return session_count

    def _create_events_for_session(
        self,
        session_start_ts: datetime,
        min_session_duration_seconds: int,
        online: bool = None,
    ) -> Tuple[EventCollection, datetime]:
        meta_events: List[MetaEvent] = []
        log_events: List[LogEvent] = []
        session_end_ts = session_start_ts + timedelta(seconds=min_session_duration_seconds)

        if self._profile_config.event_probabilities is None or len(self._profile_config.event_probabilities) == 0:
            raise ValueError(
                "No event probabilities configured for profile: %s" % (self._profile_data["profile_name"],)
            )
        current_session_ts = session_start_ts
        current_session_ts += timedelta(seconds=10 * (0.5 + random.random()))

        while current_session_ts < session_end_ts:
            selected_event_type = select_random_keys_from_dict(self._profile_config.event_probabilities, count=1)[0]
            new_events, current_session_ts = generate_event_logs_of_type(
                self, current_session_ts, selected_event_type, online
            )
            log_events.extend(new_events)

            # Random wait between event types
            current_session_ts += timedelta(seconds=random.randrange(5, 30))

        return EventCollection(meta_events=meta_events, log_events=log_events), current_session_ts

    def _generate_logout_events(self, current_session_ts: datetime, online: bool) -> Tuple[datetime, List[LogEvent]]:
        logout_events: List[LogEvent] = [IdentifyEvent(self, current_session_ts, online, action=IdentifyAction.LOGOUT)]

        current_session_ts += timedelta(seconds=get_random_float_in_range(0.5, 5))

        if self._product_user_type == ProductUserType.MOBILE:
            logout_events.append(AppEvent(self, current_session_ts, online, action=AppAction.CLOSE))
            current_session_ts += timedelta(seconds=get_random_float_in_range(0.5, 5))

        return current_session_ts, logout_events

    def _create_session_and_events(self, session_start_ts: datetime) -> EventCollection:
        online = random.random() < self._profile_config.online_probability
        engagement_session_duration_factor = self.get_profile_conf().session_engagement_duration_factor
        engagement_scaler = (
            (
                1.0
                - engagement_session_duration_factor
                + self.session_engagement_level * engagement_session_duration_factor
            )
            if ENGAGEMENT_SCALED_SESSIONS
            else 1.0
        )

        session_duration_seconds = self._profile_config.session_length_min_seconds + math.floor(
            random.random()
            * (self._profile_config.session_length_max_seconds - self._profile_config.session_length_min_seconds)
            * engagement_scaler
        )

        log_events: List[LogEvent] = []
        current_session_ts = session_start_ts

        if self._product_user_type == ProductUserType.MOBILE:
            # App opened
            log_events.append(AppEvent(self, current_session_ts, online, action=AppAction.OPEN))
            current_session_ts += timedelta(seconds=get_random_float_in_range(0.5, 5))

        # Login
        log_events.append(IdentifyEvent(self, current_session_ts, online, action=IdentifyAction.LOGIN))
        current_session_ts += timedelta(seconds=get_random_float_in_range(2, 5))

        session_events, current_session_ts = self._create_events_for_session(
            current_session_ts, session_duration_seconds, online
        )
        session_log_events = session_events.log_events

        profile_conf = self.get_profile_conf()
        background_per_minute_probability = profile_conf.background_per_minute_probability
        if self._product_user_type == ProductUserType.MOBILE and background_per_minute_probability > 0:
            session_log_events = enrich_session_events_with_backgrounding(
                session_log_events, online, background_per_minute_probability=background_per_minute_probability
            )

        log_events.extend(session_log_events)

        # Logout
        current_session_ts, logout_events = self._generate_logout_events(current_session_ts, online)
        log_events.extend(logout_events)

        rate_events, current_session_ts = generate_rate_events(
            self, current_session_ts, f"{global_conf.organisation}_{global_conf.project}", CatalogType.APP
        )
        if len(rate_events) > 0:
            log_events.extend(rate_events)

        return EventCollection(log_events=log_events)

    def prepare_after_resurrection(
        self, current_ts: datetime, resurrection_data: Dict[str, Any]
    ) -> Optional[EventCollection]:
        event_collection = None
        if "engagement_delta" in resurrection_data:
            self.get_session_engagement_manager().update_engagement(resurrection_data["engagement_delta"])
        if "nudges" in resurrection_data:
            event_collection = EventCollection()
            for nudge in resurrection_data["nudges"]:
                response_events = self.receive_nudge(nudge, current_ts)
                if response_events is not None:
                    event_collection.insert_log_event(response_events)

        return event_collection

    def is_active(self):
        return self.session_engagement_level > 10e-5

    def force_churn(self):
        self.get_session_engagement_manager().update_engagement(-100000)

    def _nudge_checks_count_today(self) -> int:
        profile_conf = self.get_profile_conf()
        return get_random_int_in_range(profile_conf.nudges.checks_per_day_min, profile_conf.nudges.checks_per_day_max)

    def _engaged_today(self, session_start_ts: datetime):
        active_dates = self._profile_data["active_dates"] if "active_dates" in self._profile_data else {}

        # Clear old data
        for dt_str, active in active_dates.copy().items():
            active_date = datetime.strptime(dt_str, "%Y-%m-%d").date()
            if active_date < (session_start_ts - timedelta(days=2)).date():
                del active_dates[dt_str]

        session_start_date_str = session_start_ts.strftime("%Y-%m-%d")
        if session_start_date_str in active_dates:
            return active_dates[session_start_date_str]

        day_of_week_offset = session_start_ts.weekday()
        day_of_week_probabilities = self.get_profile_conf().session_day_of_week_probabilities
        day_probability = day_of_week_probabilities[day_of_week_offset]
        base_session_probability = (
            GUARANTEED_ENGAGEMENT_FOR_SESSION
            + (1.0 - GUARANTEED_ENGAGEMENT_FOR_SESSION) * self.session_engagement_level
        )
        if day_probability < 1.0 and random.random() > day_probability:
            # Sessions skipped for today
            active_dates[session_start_date_str] = False
        elif random.random() <= base_session_probability:
            active_dates[session_start_date_str] = True
        else:
            active_dates[session_start_date_str] = False

        self.set_profile_data_value("active_dates", active_dates, change_ts=session_start_ts)
        return active_dates[session_start_date_str]

    def _generate_session_start_timestamps(self, start_ts: datetime, session_count: int) -> List[datetime]:
        session_start_timestamps = []
        hour_probabilities = self.get_profile_conf().session_hourly_start_probabilities
        assert len(hour_probabilities) == 24
        start_hour_probs = dict([(hour, prob) for hour, prob in enumerate(hour_probabilities)])
        start_hour_offset_probs = dict(
            [
                (
                    offset,
                    start_hour_probs[(start_ts + timedelta(hours=offset)).hour],
                )
                for offset in range(0, 24)
            ]
        )
        start_hour_offsets = select_random_keys_from_dict(start_hour_offset_probs, count=session_count)
        for start_hour_offset in start_hour_offsets:
            session_start_ts = start_ts + timedelta(hours=start_hour_offset, seconds=random.random() * 3600)
            session_start_timestamps.append(session_start_ts)

        return session_start_timestamps

    def _generate_session_events_starting_from(self, start_ts: datetime, session_count: int) -> EventCollection:
        events = EventCollection()

        if session_count <= 0:
            return events

        session_start_timestamps = self._generate_session_start_timestamps(start_ts, session_count)

        for session_index in range(0, session_count):
            session_start_ts = session_start_timestamps[session_index]

            # Have some random time pass before the start of the first session
            session_events = self._create_session_and_events(session_start_ts)
            logger.debug("Created session with %s log_events!", len(session_events.log_events))
            events.insert_events(session_events)

        return events

    def create_custom_events(self, start_ts: datetime, end_ts: datetime) -> Optional[EventCollection]:
        if not self.is_active():
            logger.debug("User not active!")
            return None

        self._currently_generating_events = EventCollection()

        if self._engaged_today(start_ts):
            logger.debug("Engaged today!")
            session_count = self._generate_session_count_for_day(start_ts)
            if session_count > 0:
                logger.debug(
                    "Generating %s sessions for %s with engagement %s...",
                    session_count,
                    self.get_platform_uuid(),
                    self.session_engagement_level,
                )

                self._currently_generating_events.insert_events(
                    self._generate_session_events_starting_from(start_ts, session_count)
                )
        else:
            logger.debug("Not engaged today!")

        # Even without being engaged on a given day, a user might encounter nudges after some time has passed since
        # registration
        eligible_for_nudges = (
            self.registration_ts.date()
            <= (start_ts - timedelta(days=global_conf.artificial_nudge_min_registration_delay_days)).date()
        )
        if global_conf.use_nudges and eligible_for_nudges:
            nudge_checks_for_day = self._nudge_checks_count_today()
            if nudge_checks_for_day > 0:
                nudge_check_timestamps = self._generate_session_start_timestamps(start_ts, nudge_checks_for_day)
                for nudge_check_timestamp in nudge_check_timestamps:
                    self._currently_generating_events.insert_meta_event(ReceiveNudges(self, nudge_check_timestamp))

                    # Generate a random and totally unrelated nudge event for synthetic data purposes
                    # NOT A REAL INTERVENTION
                    self._currently_generating_events.insert_log_event(
                        self._generate_random_nudge_response(
                            generate_random_nudge(
                                subject_id=self.get_platform_uuid(),
                                queued_at=nudge_check_timestamp
                                - timedelta(seconds=get_random_int_in_range(10, 3600 * 24)),
                            ),
                            nudge_check_timestamp,
                        )
                    )

        logger.debug(
            "Scheduled %s meta events, %s log events and %s catalog events!",
            len(self._currently_generating_events.meta_events),
            len(self._currently_generating_events.log_events),
            len(self._currently_generating_events.catalog_events),
        )

        result = self._currently_generating_events
        self._currently_generating_events = None
        return result

    def get_session_engagement_manager(self) -> EngagementManager:
        return self.get_manager("session_engagement")

    def _generate_random_nudge_response(self, nudge: Nudge, response_ts: datetime) -> NudgeResponseEvent:
        profile_conf = self.get_profile_conf()
        nudge_conf = profile_conf.nudges

        online = random.random() < self._profile_config.online_probability
        response_action: NudgeResponseAction = select_random_keys_from_dict(nudge_conf.response_probabilities, count=1)[
            0
        ]

        nudge_response = NudgeResponseEvent(
            user=self, response_ts=response_ts, online=online, nudge=nudge, nudge_response_action=response_action
        )

        return nudge_response

    def receive_nudge(self, nudge: Nudge, received_ts: datetime) -> Optional[LogEvent]:
        nudge_response = self._generate_random_nudge_response(nudge, received_ts)
        nudge_response_action = nudge_response.nudge_action
        engagement_delta: Optional[float] = None
        if nudge_response_action == NudgeResponseAction.OPEN:
            # We looked at this nudge!
            profile_conf = self.get_profile_conf()
            nudge_conf = profile_conf.nudges
            engagement_delta = generate_engagement_delta(nudge_conf.engagement_effect)
            self.get_session_engagement_manager().update_engagement(engagement_delta)

            self.set_last_seen_ts(received_ts)

        logger.info(
            "%s: Received nudge %s and responded with %s - engagement_change: %s",
            self.get_platform_uuid(),
            nudge.nudge_id,
            nudge_response_action.value,
            engagement_delta,
        )

        return nudge_response
