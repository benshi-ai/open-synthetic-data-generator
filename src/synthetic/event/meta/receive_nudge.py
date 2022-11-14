import logging
from datetime import datetime, timedelta
from typing import Optional

from synthetic.event.event_collection import EventCollection
from synthetic.event.meta.meta_base import MetaEvent
from synthetic.utils.current_time_utils import get_current_time

logger = logging.getLogger(__name__)


class ReceiveNudges(MetaEvent):
    def __init__(self, user: "SyntheticUser", ts: datetime):  # type: ignore
        super().__init__(user, ts)

    def perform_actions(self) -> Optional[EventCollection]:
        current_time = get_current_time()
        if self.ts < current_time - timedelta(days=2):
            logger.debug("Ignoring past nudge retrieval!")
            return None

        nudge_responses = self.user.retrieve_and_receive_nudges(received_ts=self.ts)

        return nudge_responses
