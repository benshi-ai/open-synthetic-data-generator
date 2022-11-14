import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from synthetic.constants import CatalogType
from synthetic.event.base import Event
from synthetic.utils.event_utils import get_external_subject_type_string

logger = logging.getLogger(__name__)


# logger.setLevel(logging.DEBUG)


class CatalogEvent(Event):
    @staticmethod
    def from_type_and_data(catalog_type: CatalogType, current_ts: datetime, data: Dict) -> "CatalogEvent":
        return CatalogEvent(catalog_type=catalog_type, ts=current_ts, data=data)

    def __init__(self, catalog_type: CatalogType, ts: datetime, data: Dict):
        super().__init__(ts)

        self._catalog_type = catalog_type

        self._data = data

    @property
    def data(self):
        return self._data

    @property
    def catalog_type(self):
        return self._catalog_type

    def get_external_subject_type(self) -> str:
        return get_external_subject_type_string(self._catalog_type)

    def as_payload_dict(self):
        payload = self._data.copy()
        payload["id"] = payload["uuid"]

        return payload

    def as_csv_dict(self):
        return {
            "ts": self.get_formatted_ts(),
            "subject_type": self.get_external_subject_type(),
            "data": json.dumps(self._data),
        }

    def get_platform_uuid(self) -> str:
        return self._data["platform_uuid"]

    def get_backend_data(self) -> Dict[str, Any]:
        raise NotImplementedError("No backend data implemented for %s: %s!" % (type(self), self._catalog_type.value))

    def get_schema_path(self) -> Optional[str]:
        return None
