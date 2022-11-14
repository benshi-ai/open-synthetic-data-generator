from datetime import datetime
from typing import Any, Dict

from synthetic.constants import CatalogType
from synthetic.event.catalog.catalog_base import CatalogEvent


class PromoCatalogEvent(CatalogEvent):
    def __init__(self, catalog_type: CatalogType, ts: datetime, data: Dict):
        super().__init__(catalog_type, ts, data)

    def as_payload_dict(self):
        payload = self.data.copy()
        payload["id"] = payload["uuid"]
        return payload

    def get_schema_path(self) -> str:
        return "promo"

    def get_backend_data(self) -> Dict[str, Any]:
        raise NotImplementedError()
