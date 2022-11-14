from datetime import datetime
import random

from synthetic.conf import EngagementConfig
from synthetic.constants import CatalogType


def prepare_price_for_writing(price: float) -> float:
    """

    :param price:
    :return:
    """
    return float(round(price, ndigits=2))


def generate_engagement_delta(config: EngagementConfig) -> float:
    engagement_changed = random.random() < config.change_probability
    if not engagement_changed:
        return 0.0

    engagement_increase = random.choices(
        [True, False], weights=[config.boost_probability, config.decay_probability], k=1
    )[0]

    engagement_delta = config.change_min + random.random() * (config.change_max - config.change_min)
    if not engagement_increase:
        engagement_delta *= -1

    return engagement_delta


def calculate_bonus_session_count(
    last_received_nudge_ts: datetime,
    session_range_start_ts: datetime,
    bonus_session_count: int = 1,
    bonus_session_days: int = 1,
) -> int:
    if last_received_nudge_ts > session_range_start_ts:
        return 0

    days_diff = max(0, (session_range_start_ts - last_received_nudge_ts).days)
    if days_diff < bonus_session_days:
        bonus_sessions = bonus_session_count
    else:
        bonus_sessions = 0
    return bonus_sessions


def get_external_subject_type_string(catalog_type: CatalogType) -> str:
    if catalog_type in [CatalogType.MEDIA_AUDIO, CatalogType.MEDIA_IMAGE, CatalogType.MEDIA_VIDEO]:
        return "media"
    else:
        return str(catalog_type.value)
