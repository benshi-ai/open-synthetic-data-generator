import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

from synthetic.user.profile_data_update import ProfileDataUpdate
from synthetic.utils.time_utils import total_difference_seconds

logger = logging.getLogger(__name__)


class BaseVariableManager:
    """Responsible for managing a variable over time. Any information placed in the passed-in "stored_data" dict will
    be persisted in the database.

    """

    def __init__(self, stored_data: Dict, initial_ts: datetime, variable_name: str, update_increment_seconds=86400):
        self._stored_data = stored_data
        self._variable_name = variable_name
        self._update_increment_seconds = update_increment_seconds

        if "last_seen_ts" not in self.data:
            self.data["last_seen_ts"] = initial_ts.timestamp()

    def get_manager_data(self):
        if "managers" not in self._stored_data:
            self._stored_data["managers"] = {}

        if self._variable_name not in self._stored_data["managers"]:
            self._stored_data["managers"][self._variable_name] = {}

        return self._stored_data["managers"][self._variable_name]

    def reset(self):
        pass

    def initialize(self):
        raise NotImplementedError()

    def update(self, current_ts: datetime) -> Optional[Dict[datetime, ProfileDataUpdate]]:
        if "last_seen_ts" not in self.data:
            raise ValueError("Variable %s not properly initialized!" % (type(self),))

        last_seen_ts = datetime.fromtimestamp(self.data["last_seen_ts"]) if "last_seen_ts" in self.data else current_ts

        updates: Optional[Dict[datetime, ProfileDataUpdate]] = None
        while total_difference_seconds(last_seen_ts, current_ts) >= self._update_increment_seconds:
            update = self.update_variable()
            last_seen_ts += timedelta(seconds=self._update_increment_seconds)

            if update is not None:
                if updates is None:
                    updates = {}

                update.add_set_variable(f"managers/{self._variable_name}/last_seen_ts", last_seen_ts.timestamp())
                updates[last_seen_ts] = update

        self.data["last_seen_ts"] = last_seen_ts.timestamp()

        return updates

    def update_variable(self) -> ProfileDataUpdate:
        raise NotImplementedError()

    @property
    def data(self):
        return self.get_manager_data()

    def set_data(self, data):
        self._stored_data = data
        self.initialize()

    @property
    def variable_name(self):
        return self._variable_name
