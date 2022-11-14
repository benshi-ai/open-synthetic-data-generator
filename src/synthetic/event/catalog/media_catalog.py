from datetime import datetime
from typing import Any, Dict

from synthetic.constants import CatalogType
from synthetic.event.catalog.catalog_base import CatalogEvent


class MediaCatalogEvent(CatalogEvent):
    def __init__(self, catalog_type: CatalogType, ts: datetime, data: Dict):
        super().__init__(catalog_type, ts, data)

    def as_payload_dict(self):
        payload = self.data.copy()
        payload["id"] = f"{self._data['media_type']}_{self._data['uuid']}"
        payload["id_source"] = self._data["uuid"]
        payload["language"] = self._data["lang"]
        payload["type"] = self._data["media_type"]

        return payload

    def get_schema_path(self) -> str:
        return "media"

    def get_backend_data(self) -> Dict[str, Any]:
        return {
            "id": f"{self._data['media_type']}_{self._data['uuid']}",
            "id_source": str(self._data["uuid"]),
            "name": str(self._data["name"]),
            "type": str(self._data["media_type"]),
            "length": str(self._data["length"]),
            "description": str(self._data["description"]),
            "resolution": str(self._data["resolution"]),
            "language": str(self._data["lang"]),
        }
