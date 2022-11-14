from copy import deepcopy
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any

import yaml

from synthetic.constants import SECONDS_IN_DAY, CatalogType, ProductUserType
from synthetic.event.constants import EventType, NudgeResponseAction
from synthetic.user.constants import SyntheticUserType

DATETIME_FIELDS = {"start_ts", "end_ts"}


def reset_configuration():
    global_conf.reset()

    global_conf.profiles = {
        "boring_guy": ProfileConfig(
            occurrence_probability=1.0,
            session_min_count=1,
            session_max_count=1,
            online_probability=0.5,
            session_engagement=EngagementConfig(change_probability=0.0, initial_min=1.0, initial_max=1.0),
            event_probabilities={EventType.PAGE: 1.0},
        )
    }


def update_dict_of_dicts(data: Dict, update_dict: Dict):
    for key, value in update_dict.items():
        if key not in data:
            data[key] = value
        elif isinstance(data[key], BaseConfig):
            data[key].update_from_dict(value.__dict__)
        elif not isinstance(value, dict):
            data[key] = value
        else:
            update_dict_of_dicts(data[key], value)


def assert_probability_range(value):
    assert 0 <= value <= 1.0


def update_object_from_dict(obj: Any, data: Dict, log_label: str):
    if data is None:
        return

    for key in data:
        if not hasattr(obj, key):
            raise ValueError(
                "Invalid %s config parameter found: %s"
                % (
                    log_label,
                    key,
                )
            )

        value = data[key]
        if key == "properties" and hasattr(obj, "properties"):
            update_dict_of_dicts(obj.properties, value)
        else:
            if key in DATETIME_FIELDS:
                value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S") if value is not None else value

            existing_value = getattr(obj, key)
            if isinstance(existing_value, BaseConfig):
                existing_value.update_from_dict(value)
            else:
                setattr(obj, key, value)


@dataclass
class BaseConfig:
    def update_from_dict(self, data: Dict):
        raise NotImplementedError()


@dataclass
class EngagementConfig(BaseConfig):
    """Configures how an engagement variable changes over time."""

    def update_from_dict(self, data: Dict):
        update_object_from_dict(self, data, log_label="engagement")

    initial_min: float = 0.0
    initial_max: float = 1.0

    change_probability: float = 0.0
    boost_probability: float = 0.25
    decay_probability: float = 0.75
    change_min: float = 1.0
    change_max: float = 1.0


@dataclass
class NudgeConfig(BaseConfig):
    def update_from_dict(self, data: Dict):
        if "response_probabilities" in data:
            new_nudge_response_probabilities = dict(
                [
                    (NudgeResponseAction(nudge_response_action_str), nudge_response_probability)
                    for nudge_response_action_str, nudge_response_probability in data["response_probabilities"].items()
                ]
            )
            self.response_probabilities = new_nudge_response_probabilities
            del data["response_probabilities"]

        update_object_from_dict(self, data, log_label="nudges")

    checks_per_day_min: int = 1
    checks_per_day_max: int = 1
    bonus_session_count: int = 1
    bonus_session_days: int = 1

    response_probabilities: Dict[NudgeResponseAction, float] = field(
        default_factory=lambda: dict([(action, 1.0) for action in NudgeResponseAction])
    )
    engagement_effect: EngagementConfig = field(default_factory=lambda: EngagementConfig())


@dataclass
class ScheduleBehaviourConfig(BaseConfig):
    def update_from_dict(self, data: Dict):
        update_object_from_dict(self, data, log_label="schedule")

    seconds_per_event: int = 60
    delivery_delay_max_days: int = 10


@dataclass
class PurchaseBehaviourConfig(BaseConfig):
    """Configure the purchasing behaviour of a user."""

    def update_from_dict(self, data: Dict):
        if "catalog_type_probabilities" in data:
            new_catalog_type_probabilities = dict(
                [
                    (CatalogType(catalog_name), catalog_probability)
                    for catalog_name, catalog_probability in data["catalog_type_probabilities"].items()
                ]
            )
            self.catalog_type_probabilities = new_catalog_type_probabilities
            del data["catalog_type_probabilities"]

        update_object_from_dict(self, data, log_label="purchase")

    initial_account_balance_min: float = 0.0
    initial_account_balance_max: float = 2000.0
    top_up_probability: float = 0.05

    payment_failure_probability_min: float = 0.0
    payment_failure_probability_max: float = 0.2
    checkout_failure_probability_min: float = 0.0
    checkout_failure_probability_max: float = 0.2
    checkout_urgent_probability_min: float = 0.0
    checkout_urgent_probability_max: float = 0.5
    checkout_cancellation_probability_min: float = 0.0
    checkout_cancellation_probability_max: float = 0.2
    checkout_promo_probability_min: float = 0.0
    checkout_promo_probability_max: float = 0.2

    interest_catalog_range_min: float = 0.1
    interest_catalog_range_max: float = 0.2
    interest_per_item_min: float = 0.5
    interest_per_item_max: float = 1.0

    views_required_per_purchase_min: int = 2
    views_required_per_purchase_max: int = 5
    views_per_session_min: int = 1
    views_per_session_max: int = 5
    impression_ratio: float = 2.0
    detail_probability: float = 0.3
    favorite_probability: float = 0.05
    reminder_probability: float = 0.05
    auto_reminder_type_probability: float = 0.05

    purchase_count_per_item_min: int = 1
    purchase_count_per_item_max: int = 5
    update_events_per_checkout_min: int = 0
    update_events_per_checkout_max: int = 5

    catalog_type_probabilities: Dict[CatalogType, float] = field(
        default_factory=lambda: dict(
            [
                (catalog_type, 1.0)
                for catalog_type in [
                    CatalogType.BLOOD,
                    CatalogType.DRUG,
                    CatalogType.OXYGEN,
                    CatalogType.MEDICAL_EQUIPMENT,
                ]
            ]
        )
    )


@dataclass
class BehaviourConfig(BaseConfig):
    """Configures fancy behaviour for a user, if applicable."""

    def update_from_dict(self, data: Dict):
        if "purchase" in data:
            self.purchase.update_from_dict(data["purchase"])
            del data["purchase"]

        update_object_from_dict(self, data, log_label="behaviour")

    normal_event_probability: float = 0.0
    purchase: PurchaseBehaviourConfig = field(default_factory=lambda: PurchaseBehaviourConfig())
    schedule: ScheduleBehaviourConfig = field(default_factory=lambda: ScheduleBehaviourConfig())


@dataclass
class EventConfig(BaseConfig):
    """Configures how events are randomly generated. Only applicable if the synthetic user implementation is generating
    fully random events.

    """

    @staticmethod
    def build_dict_from_dict(data: Dict) -> Dict[EventType, "EventConfig"]:
        event_names = sorted(list(data.keys()))

        default_config = EventConfig()
        if "default" in data:
            default_config.update_from_dict(data["default"])
            event_names.remove("default")

        events = {}
        for event_name in event_names:
            event = deepcopy(default_config)
            event_data = data[event_name]

            event.update_from_dict(event_data)
            events[EventType(event_name)] = event

        return events

    def update_from_dict(self, data: Dict):
        update_object_from_dict(self, data, log_label="event")

    properties: Dict = field(default_factory=dict)


@dataclass
class ProfileConfig(BaseConfig):
    """Configure a given profile 'type', which represents a type of user that we want to see appear in the simulation."""

    @staticmethod
    def build_config_dict_from_dict(data: Dict) -> Dict[str, "ProfileConfig"]:
        profile_names = sorted(list(data.keys()))
        default_config = ProfileConfig()
        if "default" in data:
            default_config.update_from_dict(data["default"])
            profile_names.remove("default")

        profiles = {}

        for profile_name in profile_names:
            profile = deepcopy(default_config)
            profile_data = data[profile_name]

            profile.update_from_dict(profile_data)
            profiles[profile_name] = profile

        return profiles

    def update_from_dict(self, data: Dict):
        if data is None:
            return

        if "product_user_type" in data:
            self.product_user_type = ProductUserType(data["product_user_type"])
            del data["product_user_type"]

        if "user_type" in data:
            self.user_type = SyntheticUserType(data["user_type"])
            del data["user_type"]

        if "behaviour" in data:
            self.behaviour.update_from_dict(data["behaviour"])
            del data["behaviour"]

        if "events" in data:
            new_events = EventConfig.build_dict_from_dict(data["events"])
            update_dict_of_dicts(self.events, new_events)
            del data["events"]

        if "event_probabilities" in data:
            new_event_probabilities = dict(
                [
                    (EventType(event_type_str), event_probability)
                    for event_type_str, event_probability in data["event_probabilities"].items()
                ]
            )
            update_dict_of_dicts(self.event_probabilities, new_event_probabilities)
            del data["event_probabilities"]

        update_object_from_dict(self, data, log_label="profile")

    def verify(self):
        assert self.session_min_count >= 1
        assert self.session_min_count * self.session_length_min_seconds > 0
        assert self.session_max_count * self.session_length_max_seconds <= SECONDS_IN_DAY * 0.5
        assert self.session_event_type_changes_min >= 0
        assert_probability_range(self.occurrence_probability)
        assert_probability_range(self.online_probability)
        assert_probability_range(self.session_engagement_count_factor)
        assert_probability_range(self.session_engagement_duration_factor)

        if hasattr(self, "session_engagement"):
            assert isinstance(self.session_engagement, EngagementConfig)
        if hasattr(self, "purchase_engagement"):
            assert isinstance(self.purchase_engagement, EngagementConfig)

    def get_engagement_config(self, variable_name: str) -> EngagementConfig:
        if not hasattr(self, variable_name):
            setattr(self, variable_name, EngagementConfig())

        return getattr(self, variable_name)

    def configure_early_start(self):
        self.session_hourly_start_probabilities = [0.0] * 24
        self.session_hourly_start_probabilities[3] = 1.0

    def get_event_config(self, event_type: EventType) -> EventConfig:
        if event_type not in self.events:
            self.events[event_type] = EventConfig()

        return self.events[event_type]

    user_type: SyntheticUserType = SyntheticUserType.SESSION_ENGAGEMENT

    session_min_count: int = 1
    session_max_count: int = 1
    session_event_type_changes_min: int = 0
    session_event_type_changes_max: int = 3
    session_length_min_seconds: int = 60
    session_length_max_seconds: int = 3600
    session_hourly_start_probabilities: List[float] = field(default_factory=lambda: [1.0 / 24.0] * 24)

    # Probability for realising a session based on the day of week [0: Monday, 6: Sunday]
    session_day_of_week_probabilities: List[float] = field(default_factory=lambda: [1.0] * 7)

    product_user_type: ProductUserType = ProductUserType.WEB
    background_per_minute_probability: float = 0.0

    nudges: NudgeConfig = field(default_factory=lambda: NudgeConfig())

    events: Dict[EventType, EventConfig] = field(default_factory=dict)
    event_probabilities: Dict[EventType, float] = field(default_factory=dict)

    session_engagement: EngagementConfig = field(default_factory=lambda: EngagementConfig())

    # Ratios of how much session count and duration is affected by a user's engagement
    session_engagement_count_factor: float = 0.2
    session_engagement_duration_factor: float = 0.2

    purchase_engagement: EngagementConfig = field(default_factory=lambda: EngagementConfig())
    behaviour: BehaviourConfig = field(default_factory=lambda: BehaviourConfig())

    occurrence_probability: float = 0.0
    online_probability: float = 0.2


@dataclass
class PopulationConfig(BaseConfig):
    """Configures how the population of a simulation is managed"""

    def update_from_dict(self, data: Dict):
        update_object_from_dict(self, data, log_label="population")

    initial_count: int = 0
    target_min_count: int = 100
    target_max_count: int = 300
    volatility: float = 0.05  # How volatile the population is

    resurrection_probability: float = 0.0  # The hourly probability of a single user_id getting resurrected
    inactive_nudge_check_ratio_per_hour: float = 0.0  # The hourly probability of a user_id checking for nudges
    prune_oldest_registered_users: bool = False

    def verify(self):
        assert self.initial_count >= 0
        assert self.target_min_count >= 0
        assert self.target_max_count >= 0
        assert_probability_range(self.volatility)


@dataclass
class CatalogConfig(BaseConfig):
    """Configures how a given CatalogType is randomly populated"""

    @staticmethod
    def build_config_dict_from_dict(
        data: Optional[Dict] = None,
    ) -> Dict[CatalogType, "CatalogConfig"]:
        if data is None:
            return dict([(catalog_type, CatalogConfig()) for catalog_type in CatalogType])

        catalog_types_str = sorted(list(data.keys()))

        default_config = CatalogConfig()
        if "default" in data:
            default_config.update_from_dict(data["default"])
            catalog_types_str.remove("default")

        catalogs = dict([(catalog_type, deepcopy(default_config)) for catalog_type in CatalogType])

        for catalog_type_str in catalog_types_str:
            catalog_type = CatalogType(catalog_type_str)
            catalog = catalogs[catalog_type]
            catalog_data = data[catalog_type_str]

            catalog.update_from_dict(catalog_data)

        return catalogs

    def update_from_dict(self, data: Dict):
        update_object_from_dict(self, data, log_label="catalog")

    target_count: int = 30  # The target number of unique options for this CatalogType.
    properties: Dict = field(default_factory=dict)  # Some custom properties that can be used during catalog gen.


@dataclass
class GlobalConfig(BaseConfig):
    """The root configuration of the entire simulation."""

    env: str = "local"
    api_url: str = "http://www.test.com"  # The url used for calls to the API, if applicable
    api_key: str = "unknown"  # The API key used for calls to the API, if applicable
    notify: bool = False

    simulation_profile: str = "acme"  # The entity that's being simulated - might allow us to use some prefab data.
    organisation: str = "demo"
    project: str = "demo"

    sink_types: List[str] = field(
        default_factory=list
    )  # The type of driver that is used, e.g. memory, http (for API) or csv
    start_ts: datetime = datetime(2020, 1, 1)
    end_ts: Optional[datetime] = datetime(2020, 2, 1)
    db_uri: str = "sqlite:///:memory:"  # The URI to the db that is used to maintain the state of the simulation

    log_events_filename: Optional[
        str
    ] = None  # The filename to which log events are dumped in the case of a 'csv' driver
    catalog_events_filename: Optional[
        str
    ] = None  # The filename to which catalog events are dumped in the case of a 'csv' driver

    # Configures how the population is managed in the simulation.
    population: PopulationConfig = field(default_factory=lambda: PopulationConfig())

    # Profile names mapped to profile configs which define the various 'types' of people that will appear in the
    # simulation and how probable they are to appear.
    profiles: Dict[str, ProfileConfig] = field(default_factory=dict)

    # Catalog types mapped to configurations defining how the catalog is generated for the given type.
    catalogs: Dict[CatalogType, CatalogConfig] = field(default_factory=dict)

    filter_log_events_for_csv: bool = False

    randomise_registration_times: bool = False
    manage_population_counts_per_profile: bool = False

    rating_probability: float = 0.01
    use_nudges: bool = False
    use_promotions: bool = False
    artificial_nudge_min_registration_delay_days: int = 7

    cache_logs_on_failure: bool = True

    def update_from_dict(self, data: Dict):
        update_object_from_dict(self, data, log_label="global")

    def load_from_yaml(self, yaml_filename: str):
        with open(yaml_filename, "r") as yaml_file:
            config_data = yaml.safe_load(yaml_file)
            global_data = config_data["global"]

            # First load globals
            self.update_from_dict(global_data)

            # Then load users
            user_data = config_data["users"]
            self.population = PopulationConfig()
            self.population.update_from_dict(user_data["population"])

            self.profiles = ProfileConfig.build_config_dict_from_dict(user_data["profiles"])

            self.catalogs = CatalogConfig.build_config_dict_from_dict(config_data.get("catalogs", None))

        self.verify()

    def reset(self):
        new_config = GlobalConfig()
        self.__dict__ = new_config.__dict__

    def verify(self):
        self.population.verify()

        for profile in self.profiles.values():
            profile.verify()

    def get_catalog_config(self, catalog_type: CatalogType) -> CatalogConfig:
        if self.catalogs is None:
            self.catalogs = {}
        if catalog_type not in self.catalogs:
            self.catalogs[catalog_type] = CatalogConfig()

        return self.catalogs[catalog_type]


global_conf = GlobalConfig()
