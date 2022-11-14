import logging
from datetime import datetime
from typing import Dict

from synthetic.conf import EngagementConfig
from synthetic.managers.base_manager import BaseVariableManager
from synthetic.user.profile_data_update import ProfileDataUpdate
from synthetic.utils.event_utils import generate_engagement_delta
from synthetic.utils.random import get_random_float_in_range

logger = logging.getLogger(__name__)


# logger.setLevel(logging.DEBUG)


class EngagementManager(BaseVariableManager):
    """A generic engagement manager that is designed to represent some form of engagement, e.g. session- or
    purchase-engagement on a user.

    """

    def __init__(self, stored_data: Dict, config: EngagementConfig, initial_ts: datetime, variable_name: str):
        super().__init__(stored_data, initial_ts, variable_name)

        self._config = config

    def initialize(self):
        if "engagement_level" not in self.data:
            self.reset()

    def reset(self):
        super().reset()
        self.data["engagement_level"] = get_random_float_in_range(self._config.initial_min, self._config.initial_max)

    def update_variable(self) -> ProfileDataUpdate:
        logger.debug("Updating engagement")

        if self.data["engagement_level"] < 10e-5:
            # That's the end of engagement
            self.data["engagement_level"] = 0.0

            return ProfileDataUpdate.create_variable_set_update(f"managers/{self._variable_name}/engagement_level", 0.0)

        engagement_delta = generate_engagement_delta(self._config)
        logger.debug("Updating variable with engagement delta %s", engagement_delta)
        updated_engagement = self.update_engagement(engagement_delta)

        return ProfileDataUpdate.create_variable_set_update(
            f"managers/{self._variable_name}/engagement_level", updated_engagement
        )

    def get_engagement(self) -> float:
        return self.data["engagement_level"]

    def _get_updated_engagement(self, engagement_delta: float):
        return max(min(self.get_engagement() + engagement_delta, 1.0), 0.0)

    def update_engagement(self, engagement_delta: float) -> float:
        logger.debug("Updating engagement by %s", engagement_delta)
        updated_engagement = self.data["engagement_level"] = self._get_updated_engagement(engagement_delta)
        return updated_engagement
