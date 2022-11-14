from datetime import datetime
from random import randrange
from typing import Any, Dict, List

from synthetic.constants import BlockType
from synthetic.event.base import Event
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.commerce.constants import ItemType
from synthetic.utils.data_utils import prepare_data_for_db


class LogEvent(Event):
    def __init__(
        self,
        user: "SyntheticUser",  # type: ignore
        ts: datetime,
        online: bool,
        event_type: str,
        props: Dict[str, Any] = None,
        block: BlockType = BlockType.CORE,
    ):
        super().__init__(ts)

        self.user = user
        self.device_id = user.get_current_device_id()
        self.online = online
        self.event_type = event_type
        self.props = props if props is not None else {}
        self.block = block

        self._up = randrange(1000, 100000)
        self._dn = randrange(1000, 100000)

    def __str__(self):
        return "%s - %s: %s (%s)" % (
            self.ts,
            self.user.get_platform_uuid(),
            self.event_type,
            self.online,
        )

    def as_csv_dict(self):
        return self.as_payload_dict()

    def as_payload_dict(self):
        return {
            "u_id": self.user.get_platform_uuid(),
            "d_id": self.device_id,
            "os": "android",
            "ol": self.online,
            "ts": self.get_formatted_ts(),
            "type": self.event_type,
            "ip": "0.0.0.0",
            "up": self._up,
            "dn": self._dn,
            "block": self.block.value,
            "props": prepare_data_for_db(self.props),
        }

    def update_driver_after_flush(self, driver: "Driver"):  # type: ignore
        pass

    def generate_associated_catalog_events(self) -> List[CatalogEvent]:
        return []

    def get_associated_item_types(self) -> List[ItemType]:
        return []
