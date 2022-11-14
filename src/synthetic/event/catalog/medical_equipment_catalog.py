from datetime import datetime
from typing import Any, Dict

from synthetic.constants import CatalogType
from synthetic.event.catalog.catalog_base import CatalogEvent


class MedicalEquipmentCatalogEvent(CatalogEvent):
    def __init__(self, catalog_type: CatalogType, ts: datetime, data: Dict):
        super().__init__(catalog_type, ts, data)

    def as_payload_dict(self):
        payload = self.data.copy()
        payload["id"] = payload["uuid"]
        return payload

    def get_schema_path(self) -> str:
        return "medical_equipment"

    def get_backend_data(self) -> Dict[str, Any]:
        return {
            "id": str(self._data["uuid"]),
            "name": str(self._data["name"]),
            "description": str(self._data["description"]),
            "market_id": str(self._data["market_id"]),
            "supplier_id": str(self._data["supplier_id"]),
            "supplier_name": str(self._data["supplier_name"]),
            "producer": str(self._data["producer"]),
            "packaging": str(self._data["packaging"]),
            "packaging_size": float(self._data["packaging_size"]),
            "packaging_units": str(self._data["packaging_units"]),
            "category": str(self._data["category"]),
        }
