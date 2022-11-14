from datetime import datetime
from enum import Enum
from typing import List

from synthetic.constants import BlockType, CatalogType
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.catalog_utils import data_as_catalog_event

MIN_MILESTONE_SCORE = 50.0


class MilestoneAction(Enum):
    ACHIEVED = "achieved"


class MilestoneEvent(LogEvent):
    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        milestone_id: str,
        action: MilestoneAction,
    ):
        super().__init__(
            user, ts, online, "milestone", {"id": milestone_id, "action": action.value}, block=BlockType.LOYALTY
        )

        self._milestone_id = milestone_id

    def get_schema_path(self) -> str:
        return "events/milestone"

    def generate_associated_catalog_events(self) -> List[CatalogEvent]:
        return [data_as_catalog_event(CatalogType.MILESTONE, self._milestone_id, self.ts)]
