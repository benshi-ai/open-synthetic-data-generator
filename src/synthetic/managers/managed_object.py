from datetime import datetime
import logging
from typing import Dict

from synthetic.managers.base_manager import BaseVariableManager
from synthetic.user.profile_data_update import ProfileDataUpdate

logger = logging.getLogger(__name__)


# logger.setLevel(logging.DEBUG)


class ManagedObject:
    """Generic class for managing variable managers."""

    def __init__(self):
        self._managers: Dict[str, BaseVariableManager] = {}

    def update_managers(self, current_ts: datetime) -> Dict[datetime, ProfileDataUpdate]:
        updates = {}

        for manager in self._managers.values():
            manager_updates = manager.update(current_ts)
            if manager_updates is None:
                continue

            updates.update(manager_updates)

        return updates

    def update_manager_with_data(self, variable_name: str, manager_data: Dict):
        self._managers[variable_name].set_data(manager_data)

    def add_manager(self, manager: BaseVariableManager):
        assert manager.variable_name not in self._managers

        self._managers[manager.variable_name] = manager

    def get_manager(self, variable_name: str):
        return self._managers.get(variable_name, None)

    def set_manager_data(self, data):
        for manager in self._managers.values():
            manager.set_data(data)

    def initialize_all(self):
        for manager in self._managers.values():
            manager.initialize()
