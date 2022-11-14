import logging
import random
from typing import Dict, Any, List

from synthetic.conf import ProfileConfig
from synthetic.constants import SECONDS_IN_DAY

logger = logging.getLogger(__name__)


def get_random_delivery_delay_seconds(delivery_delay_max_days: int, is_urgent: bool) -> float:
    # Cannot deliver faster than 1 day
    if not is_urgent:
        delay = SECONDS_IN_DAY + delivery_delay_max_days * SECONDS_IN_DAY * abs(random.normalvariate(0, 0.1))
        delay -= SECONDS_IN_DAY * abs(random.normalvariate(0, 0.2))
    else:
        delay = 3600 * (1.0 + abs(random.random() * 4.0))

    return delay


def get_random_float_in_range(min_value: float, max_value: float) -> float:
    return min_value + random.random() * (max_value - min_value)


def get_random_int_in_range(min_value: int, max_value: int):
    if max_value <= min_value:
        return min_value

    return random.randrange(min_value, max_value)


def select_random_keys_from_dict(data: Dict[Any, Any], count: int = 1) -> List[Any]:
    names = []
    weights = []

    for name, name_conf in data.items():
        names.append(name)
        if isinstance(name_conf, float):
            weights.append(name_conf)
        else:
            weights.append(name_conf.occurrence_probability)

    if len(names) == 0 or sum(weights) < 10e-5:
        raise ValueError("Nothing to sample in config! %s" % (data,))

    result = random.choices(names, weights=weights, k=count)
    return result


def normalise_probabilities(probabilities):
    total_probability = sum(probabilities.values())
    raw_profile_probabilities = dict(
        [(profile_name, probability / total_probability) for profile_name, probability in probabilities.items()]
    )

    return raw_profile_probabilities


def build_need_based_profile_probabilities(
    desired_population_count: int, existing_profile_counts: Dict[str, int], profiles: Dict[str, ProfileConfig]
):
    raw_profile_probabilities: Dict[str, float] = {}
    for profile_name, profile in profiles.items():
        raw_profile_probabilities[profile_name] = profile.occurrence_probability
    if sum(raw_profile_probabilities.values()) < 10e-5:
        # Everything seems saturated already, return original distribution
        return dict([(profile_name, profile.occurrence_probability) for profile_name, profile in profiles.items()])
    raw_profile_probabilities = normalise_probabilities(raw_profile_probabilities)

    desired_profile_counts = dict(
        [
            (profile_name, round(raw_profile_probabilities[profile_name] * desired_population_count))
            for profile_name in profiles
        ]
    )

    corrected_profile_probabilities = {}
    for profile_name, raw_probability in raw_profile_probabilities.items():
        if desired_profile_counts[profile_name] <= 0:
            continue

        existing_profile_count = existing_profile_counts[profile_name] if profile_name in existing_profile_counts else 0
        profile_shortage = (desired_profile_counts[profile_name] - existing_profile_count) / desired_profile_counts[
            profile_name
        ]
        corrected_profile_probabilities[profile_name] = max(0.0, raw_probability * profile_shortage)

    if sum(corrected_profile_probabilities.values()) < 10e-5:
        # Everything seems saturated already, return original distribution
        return raw_profile_probabilities

    return normalise_probabilities(corrected_profile_probabilities)


def select_random_profile_names_based_on_counts(
    desired_population_count: int,
    existing_profile_counts: Dict[str, int],
    profiles: Dict[str, ProfileConfig],
    generated_count: int = 1,
) -> List[str]:
    need_based_profile_probabilities = build_need_based_profile_probabilities(
        desired_population_count, existing_profile_counts, profiles
    )
    logger.debug("Probabilities:")
    for key, prob in need_based_profile_probabilities.items():
        logger.debug("%s: %s", key, prob)
    return select_random_keys_from_dict(need_based_profile_probabilities, count=generated_count)


def generate_random_rate_value() -> float:
    return 1.0 + random.random() * 4.0
