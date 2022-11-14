import logging
import random
from datetime import datetime
from typing import Dict

from synthetic.conf import PopulationConfig
from synthetic.managers.base_manager import BaseVariableManager
from synthetic.user.profile_data_update import ProfileDataUpdate

logger = logging.getLogger(__name__)


# logger.setLevel(logging.DEBUG)


class PopulationManager(BaseVariableManager):
    """A manager for keeping track of a population"""

    def __init__(
        self, stored_data: Dict, config: PopulationConfig, initial_ts: datetime, variable_name: str = "population"
    ):
        super().__init__(stored_data, initial_ts, variable_name)

        self._config: PopulationConfig = config

    def initialize(self):
        if "current_population" not in self.data:
            self.reset()

    def reset(self):
        super().reset()
        self.data["current_population"] = self._config.initial_count

    def update_variable(self) -> ProfileDataUpdate:
        if self._config.target_max_count <= self._config.target_min_count:
            self.data["current_population"] = self._config.target_min_count

            return ProfileDataUpdate.create_variable_set_update(
                f"managers/{self._variable_name}/current_population", self._config.target_min_count
            )

        logger.debug("Updating population")

        current_population = self.data["current_population"]

        # Every day the population count changes
        population_ratio = (current_population - self._config.target_min_count) / (
            self._config.target_max_count - self._config.target_min_count
        )

        if 0.2 < population_ratio < 0.5:
            increase_probability = 0.4
        elif 0.5 < population_ratio < 0.8:
            increase_probability = 0.6
        else:
            increase_probability = 1.0 - population_ratio

        if random.random() < increase_probability:
            # Population increases
            change_ratio = 1.0 - population_ratio
        else:
            # Population decreases
            change_ratio = -population_ratio

        change_amount = round(
            random.random()
            * (self._config.target_max_count - self._config.target_min_count)
            * self._config.volatility
            * change_ratio
        )

        current_population += change_amount
        logger.debug(
            "New population: %s (min %s, max %s)",
            current_population,
            self._config.target_min_count,
            self._config.target_max_count,
        )

        self.data["current_population"] = current_population

        return ProfileDataUpdate.create_variable_set_update(
            f"managers/{self._variable_name}/current_population", current_population
        )

    def get_population(self) -> int:
        return self.data["current_population"]
