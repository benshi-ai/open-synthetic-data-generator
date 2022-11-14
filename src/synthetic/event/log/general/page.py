from datetime import datetime
from typing import List

from synthetic.constants import BlockType, CatalogType
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.catalog_utils import data_as_catalog_event


class PageEvent(LogEvent):
    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        uuid: str,
        path: str,
        title: str,
        duration: float,  # Seconds
        block: BlockType = BlockType.CORE,
    ):
        super().__init__(user, ts, online, "page", {"path": path, "title": title, "duration": duration}, block=block)
        self._uuid = uuid

    def get_schema_path(self) -> str:
        return "events/page"

    def generate_associated_catalog_events(self) -> List[CatalogEvent]:
        return [data_as_catalog_event(CatalogType.PAGE, self._uuid, self.ts)]
