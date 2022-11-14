import logging
from datetime import datetime
from enum import Enum
from typing import List

from synthetic.constants import BlockType, CatalogType
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.catalog_utils import data_as_catalog_event

logger = logging.getLogger(__name__)


# logger.setLevel(logging.DEBUG)


class ModuleAction(Enum):
    VIEW = "view"


class ModuleEvent(LogEvent):
    def __init__(
        self, user: SyntheticUser, ts: datetime, online: bool, module_id: str, action: ModuleAction, progress: int
    ):
        super().__init__(
            user,
            ts,
            online,
            "module",
            {"id": module_id, "action": action.value, "progress": progress},
            block=BlockType.ELEARNING,
        )

        self._module_id = module_id

    def __str__(self):
        return "%s - %s: %s (%s), %s" % (
            self.ts,
            self.user.get_platform_uuid(),
            self.event_type,
            self.online,
            self.props["action"],
        )

    def get_schema_path(self) -> str:
        return "events/module"

    def generate_associated_catalog_events(self) -> List[CatalogEvent]:
        return [data_as_catalog_event(CatalogType.MODULE, self._module_id, self.ts)]
