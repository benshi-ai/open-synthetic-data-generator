from typing import Dict, Optional, Any

from synthetic.database.db_session_wrapper import DBSessionWrapper
from synthetic.database.schemas import DriverMetaSchema
from synthetic.utils.database import load_driver_meta_from_db


class DatabaseCache:
    driver_metas: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def clear(cls):
        cls.driver_metas = {}

    @classmethod
    def store_driver_meta(
        cls, organisation: str, project: str, driver_meta_data: Dict[str, Any], db_session: DBSessionWrapper
    ) -> Dict[str, Any]:
        driver_meta = load_driver_meta_from_db(db_session=db_session, organisation=organisation, project=project)
        if driver_meta is None:
            driver_meta = DriverMetaSchema(
                organisation=organisation,
                project=project,
                last_seen_ts=driver_meta_data["last_seen_ts"],
                last_maintenance_ts=driver_meta_data["last_maintenance_ts"],
                driver_data=driver_meta_data["driver_data"],
            )
        else:
            driver_meta.last_seen_ts = driver_meta_data["last_seen_ts"]
            driver_meta.last_maintenance_ts = driver_meta_data["last_maintenance_ts"]
            driver_meta.driver_data = driver_meta_data["driver_data"]

        db_session.add(driver_meta)
        db_session.commit()

        driver_meta_data["id"] = driver_meta.id
        cls.driver_metas["%s_%s" % (organisation, project)] = driver_meta_data

        return driver_meta_data

    @classmethod
    def get_driver_meta(cls, organisation: str, project: str, db_session: DBSessionWrapper) -> Optional[Dict[str, Any]]:
        org_proj = "%s_%s" % (organisation, project)
        if org_proj in cls.driver_metas:
            return cls.driver_metas[org_proj]

        loaded_driver_meta: Optional[DriverMetaSchema] = None
        if org_proj not in cls.driver_metas:
            loaded_driver_meta = load_driver_meta_from_db(db_session, organisation, project)

        if loaded_driver_meta is None:
            return None

        loaded_driver_meta_data = {
            "id": loaded_driver_meta.id,
            "last_seen_ts": loaded_driver_meta.last_seen_ts,
            "last_maintenance_ts": loaded_driver_meta.last_maintenance_ts,
            "driver_data": loaded_driver_meta.driver_data,
        }
        cls.store_driver_meta(organisation, project, loaded_driver_meta_data, db_session=db_session)
        return cls.driver_metas[org_proj]
