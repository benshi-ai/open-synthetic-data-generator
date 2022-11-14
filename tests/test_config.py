import os

from synthetic import TEST_DATA_DIRNAME
from synthetic.conf import global_conf, EngagementConfig, NudgeConfig
from synthetic.constants import CatalogType, ProductUserType
from synthetic.event.constants import EventType, NudgeResponseAction
from synthetic.user.constants import SyntheticUserType


def test_config_with_inherited_defaults():
    global_conf.load_from_yaml(os.path.join(TEST_DATA_DIRNAME, "config", "inherited_user.yaml"))

    assert global_conf.api_key == "a very secret key"

    assert global_conf.population.initial_count == 200

    assert len(global_conf.profiles) == 1
    assert global_conf.profiles["simple"].session_length_max_seconds == 1800
    assert global_conf.profiles["simple"].occurrence_probability == 0.05

    assert len(global_conf.profiles["simple"].events) == 0
    assert global_conf.profiles["simple"].event_probabilities[EventType.PAGE] == 0.7
    assert global_conf.profiles["simple"].event_probabilities[EventType.VIDEO] == 0.0


def test_example_session_engagement_config_parsing():
    global_conf.load_from_yaml(os.path.join(TEST_DATA_DIRNAME, "config", "session_engagement_user.yaml"))
    assert global_conf.api_key == "a very secret key"

    one_time_profile_config = global_conf.profiles["one_time"]
    short_profile_config = global_conf.profiles["short"]

    assert one_time_profile_config.product_user_type == ProductUserType.MOBILE
    assert one_time_profile_config.background_per_minute_probability == 0.05
    assert one_time_profile_config.event_probabilities == {
        EventType.AUDIO: 0.05,
        EventType.IMAGE: 0.05,
        EventType.MODULE: 0.1,
        EventType.PAGE: 0.5,
        EventType.VIDEO: 0.25,
    }

    assert isinstance(one_time_profile_config.session_engagement, EngagementConfig)
    assert isinstance(one_time_profile_config.purchase_engagement, EngagementConfig)
    assert isinstance(one_time_profile_config.nudges, NudgeConfig)

    assert one_time_profile_config.nudges.checks_per_day_min == 1
    assert one_time_profile_config.nudges.checks_per_day_max == 3
    assert one_time_profile_config.nudges.response_probabilities == {
        NudgeResponseAction.OPEN: 0.5,
        NudgeResponseAction.BLOCK: 0.25,
    }
    assert one_time_profile_config.nudges.engagement_effect.boost_probability == 0.75

    assert short_profile_config.nudges.checks_per_day_min == 2
    assert short_profile_config.nudges.checks_per_day_max == 3
    assert short_profile_config.nudges.response_probabilities == {
        NudgeResponseAction.OPEN: 1.0,
        NudgeResponseAction.DISCARD: 1.0,
        NudgeResponseAction.BLOCK: 1.0,
    }
    assert short_profile_config.nudges.engagement_effect.boost_probability == 0.25

    video_event_config = one_time_profile_config.events[EventType.VIDEO]
    assert video_event_config.properties == {"pause_probability": 0.3}

    shop_item_catalog_config = global_conf.get_catalog_config(CatalogType.DRUG)
    assert shop_item_catalog_config.target_count == 10
    assert shop_item_catalog_config.properties == {}

    module_item_catalog_config = global_conf.get_catalog_config(CatalogType.MODULE)
    assert module_item_catalog_config.target_count == 20
    assert module_item_catalog_config.properties == {'length_max_seconds': 120, 'length_min_seconds': 60}

    assert global_conf.population.resurrection_probability == 0.05
    assert global_conf.population.inactive_nudge_check_ratio_per_hour == 0.05


def test_example_purchase_engagement_config_parsing():
    global_conf.load_from_yaml(os.path.join(TEST_DATA_DIRNAME, "config", "purchase_engagement_user.yaml"))

    one_time_profile_config = global_conf.profiles["one_time"]
    assert one_time_profile_config.user_type == SyntheticUserType.PURCHASE_ENGAGEMENT  # From default

    short_profile_config = global_conf.profiles["short"]
    assert short_profile_config.user_type == SyntheticUserType.SESSION_ENGAGEMENT  # Override default

    assert len(one_time_profile_config.event_probabilities) == 0

    assert global_conf.get_catalog_config(CatalogType.MODULE).target_count == 10
    assert global_conf.get_catalog_config(CatalogType.DRUG).target_count == 50

    assert one_time_profile_config.behaviour.purchase.interest_catalog_range_min == 0.2
    assert one_time_profile_config.behaviour.purchase.interest_catalog_range_max == 0.3
    assert one_time_profile_config.behaviour.purchase.interest_per_item_min == 0.1
    assert one_time_profile_config.behaviour.purchase.interest_per_item_max == 0.2
    assert one_time_profile_config.behaviour.purchase.catalog_type_probabilities == {CatalogType.OXYGEN: 1.0}

    assert short_profile_config.behaviour.purchase.interest_catalog_range_min == 0.8
    assert short_profile_config.behaviour.purchase.interest_catalog_range_max == 0.9
    assert short_profile_config.behaviour.purchase.interest_per_item_min == 0.1
    assert short_profile_config.behaviour.purchase.interest_per_item_max == 0.2
    assert short_profile_config.behaviour.purchase.catalog_type_probabilities == {CatalogType.BLOOD: 1.0}
