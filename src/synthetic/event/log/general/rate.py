from datetime import datetime
from typing import List

from synthetic.constants import BlockType, CatalogType
from synthetic.event.log.commerce.constants import ItemType
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.event_utils import get_external_subject_type_string


class RateEvent(LogEvent):
    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        subject_id: str,
        catalog_type: CatalogType,
        rate_value: float,
        block: BlockType = BlockType.CORE,
    ):
        super().__init__(
            user,
            ts,
            online,
            "rate",
            {
                "subject_id": subject_id,
                "type": get_external_subject_type_string(catalog_type),
                "rate_value": float(rate_value),
            },
            block=block,
        )

        self._catalog_type = catalog_type

    def get_schema_path(self) -> str:
        return "events/rate"

    def get_associated_item_types(self) -> List[ItemType]:
        try:
            return [ItemType(self._catalog_type.value)]
        except ValueError:
            return []
