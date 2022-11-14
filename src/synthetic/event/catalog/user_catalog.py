from datetime import datetime
from typing import Any, Dict

from synthetic.constants import CatalogType
from synthetic.event.catalog.catalog_base import CatalogEvent


class UserCatalogEvent(CatalogEvent):
    def __init__(self, ts: datetime, data: Dict):
        super().__init__(CatalogType.USER, ts, data)

    def as_payload_dict(self):
        payload = self.data.copy()
        payload["id"] = payload["platform_uuid"]
        return payload

    def get_schema_path(self) -> str:
        return "user"

    def get_backend_data(self) -> Dict[str, Any]:
        return {
            "id": str(self._data["platform_uuid"]),
            "name": str(self._data["name"]),
            "country": str(self._data["country"]),
            "region_state": str(self._data["region_state"]),
            "city": str(self._data["city"]),
            "workplace": str(self._data["workplace"]),
            "timezone": str(self._data["timezone"]),
            "profession": str(self._data["profession"]),
            "zipcode": str(self._data["zipcode"]),
            "language": str(self._data["language"]),
            "experience": str(self._data["experience"]),
            "education_level": str(self._data["education_level"]),
            "organization": str(self._data["organization"]),
        }
