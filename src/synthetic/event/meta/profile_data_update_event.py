import logging
from datetime import datetime
from typing import Optional

from synthetic.event.event_collection import EventCollection
from synthetic.event.meta.meta_base import MetaEvent
from synthetic.user.profile_data_update import ProfileDataUpdate

logger = logging.getLogger(__name__)


class ProfileDataUpdateEvent(MetaEvent):
    def __init__(self, user: "SyntheticUser", ts: datetime, update: ProfileDataUpdate):  # type: ignore
        super().__init__(user, ts)

        self._update = update

    def perform_actions(self) -> Optional[EventCollection]:
        self._update.apply_to_user(self.user)
        return None
