from datetime import datetime
from typing import Any, Dict

from synthetic.constants import CatalogType
from synthetic.event.catalog.catalog_base import CatalogEvent


class DrugCatalogEvent(CatalogEvent):
    def __init__(self, catalog_type: CatalogType, ts: datetime, data: Dict):
        super().__init__(catalog_type, ts, data)

    def as_payload_dict(self):
        payload = self.data.copy()
        payload["id"] = payload["uuid"]
        payload["name"] = payload["drug_name"]
        return payload

    def get_schema_path(self) -> str:
        return "drug"

    def get_backend_data(self) -> Dict[str, Any]:
        return {
            "id": str(self._data["uuid"]),
            "name": str(self._data["drug_name"]),
            "market_id": str(self._data["market_id"]),
            "description": str(self._data["description"]),
            "supplier_id": str(self._data["supplier_id"]),
            "supplier_name": str(self._data["supplier_name"]),
            "active_ingredients": str(self._data["active_ingredients"]),
            "producer": str(self._data["producer"]),
            "packaging": str(self._data["packaging"]),
            "drug_name": str(self._data["drug_name"]),
            "drug_form": str(self._data["drug_form"]),
            "drug_strength": str(self._data["drug_strength"]),
            "atc_anatomical_group": str(self._data["atc_anatomical_group"]),
            "otc_or_ethical": str(self._data["otc_or_ethical"]),
        }
