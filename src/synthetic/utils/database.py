import logging
import resource
import uuid
from datetime import datetime
from typing import List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from synthetic.catalog.generator import create_predefined_catalog_events_for_type
from synthetic.conf import global_conf
from synthetic.database.db_session_wrapper import DBSessionWrapper
from synthetic.database.schemas import (
    DriverMetaSchema,
    SyntheticUserSchema,
    CatalogEntrySchema,
)
from synthetic.constants import CatalogType
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.utils.data_utils import prepare_data_for_db
from synthetic.utils.user_utils import fake

logger = logging.getLogger(__name__)


def clear_db_data(db_session: DBSessionWrapper):
    driver_meta = load_driver_meta_from_db(db_session, global_conf.organisation, global_conf.project)

    db_session.query(SyntheticUserSchema).filter_by(driver_meta=driver_meta).delete()
    db_session.query(CatalogEntrySchema).filter_by(driver_meta=driver_meta).delete()
    db_session.query(DriverMetaSchema).filter_by(
        organisation=global_conf.organisation, project=global_conf.project
    ).delete()
    db_session.commit()


def load_driver_meta_from_db(
    db_session: DBSessionWrapper, organisation: str, project: str
) -> Optional[DriverMetaSchema]:
    return db_session.query(DriverMetaSchema).filter_by(organisation=organisation, project=project).first()


def create_db_session(db_uri: str = None) -> DBSessionWrapper:
    if db_uri is None:
        db_uri = global_conf.db_uri

    db_engine = create_engine(db_uri)
    db_session = sessionmaker(bind=db_engine)()
    return DBSessionWrapper(db_session)


def store_catalogs_in_db(db_session: DBSessionWrapper, driver_meta_id: int, catalogs: List[CatalogEvent]):
    for catalog in catalogs:
        catalog_data = catalog.data
        db_session.add(
            CatalogEntrySchema(
                type=catalog.catalog_type.value,
                driver_meta_id=driver_meta_id,
                platform_uuid=catalog.data["uuid"],
                data=prepare_data_for_db(catalog_data),
            )
        )
    db_session.commit()


def populate_catalog_to_count_in_db(
    db_session: DBSessionWrapper,
    driver_meta_id: int,
    catalog_type: CatalogType,
    target_count: int,
    current_ts: datetime,
) -> List[CatalogEvent]:
    assert isinstance(driver_meta_id, int)
    from synthetic.catalog.generator import create_random_catalog_events_for_type

    existing_catalog_ids = db_session.query(CatalogEntrySchema.id, CatalogEntrySchema.type).filter_by(
        driver_meta_id=driver_meta_id, type=catalog_type.value
    )
    existing_count = existing_catalog_ids.count()

    new_catalogs = []
    if existing_count == 0:
        # We add any pre-defined catalogs for the simulation_profile.
        catalogs: List[CatalogEvent] = create_predefined_catalog_events_for_type(
            global_conf.simulation_profile, catalog_type, current_ts
        )
        new_catalogs.extend(catalogs)

        existing_count = len(new_catalogs)

    new_count = target_count - existing_count
    logger.info("%s: Target %s, existing %s", catalog_type, target_count, existing_count)
    if new_count > 0:
        logger.debug("Generating %s new catalogs for %s!", new_count, catalog_type)
        if catalog_type == CatalogType.MILESTONE:
            from synthetic.event.log.loyalty.milestone import MIN_MILESTONE_SCORE

            required_level_scores: List[float] = [MIN_MILESTONE_SCORE]
            for _ in range(1, new_count):
                required_level_scores.append(required_level_scores[-1] * 1.5)

            for required_level_score in required_level_scores:
                new_catalogs.append(
                    CatalogEvent(
                        catalog_type,
                        current_ts,
                        {
                            "uuid": str(uuid.uuid4()),
                            "name": fake.sentence(),
                            "required_score": required_level_score,
                        },
                    )
                )
        else:
            for _ in range(0, new_count):
                catalogs = create_random_catalog_events_for_type(catalog_type, current_ts)
                new_catalogs.extend(catalogs)

    if len(new_catalogs) > 0:
        store_catalogs_in_db(db_session, driver_meta_id, new_catalogs)

    return new_catalogs


def get_current_memory_usage_kb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
