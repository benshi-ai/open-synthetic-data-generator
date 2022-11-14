import logging
import sys
from datetime import datetime
from typing import List, Any, Dict

from IPython import embed
from sqlalchemy.orm.attributes import flag_modified

from synthetic.catalog.cache import CatalogCache
from synthetic.catalog.generator import create_random_category_data_for_item_type, create_catalog_event_for_type
from synthetic.conf import global_conf
from synthetic.constants import CatalogType
from synthetic.database.db_session_wrapper import DBSessionWrapper
from synthetic.database.schemas import SyntheticUserSchema, CatalogEntrySchema
from synthetic.driver.driver import Driver
from synthetic.event.event_collection import EventCollection
from synthetic.event.log.commerce.constants import ItemType
from synthetic.utils.database import load_driver_meta_from_db, create_db_session

logger = logging.getLogger(__name__)


def update_catalog_data(db_session: DBSessionWrapper, item_type: ItemType, all_catalog_entries):
    catalog_entries = all_catalog_entries.filter_by(type=item_type.value)
    assert catalog_entries.count() > 0

    for catalog_entry in catalog_entries.all():
        new_data = create_random_category_data_for_item_type(item_type)
        if "currency" in new_data:
            new_data["currency"] = new_data["currency"].value
        catalog_entry.data.update(new_data)
        catalog_entry.data["uuid"] = catalog_entry.platform_uuid
        flag_modified(catalog_entry, "data")

        db_session.add(catalog_entry)
        db_session.commit()


def flush_catalog_data():
    logger.info("Initializing driver...")
    driver = Driver(sink_types=["http"])
    driver.initialize_from_db()

    log_ts = datetime.now()
    for catalog_type in CatalogType:
        catalog_datas: List[Dict[str, Any]] = CatalogCache.get_all_catalogs(catalog_type)
        if len(catalog_datas) == 0:
            logger.info("Skipping %s with no catalogs...", catalog_type.value)
            continue

        logger.info("Generating events for %s at %s...", catalog_type.value, log_ts)
        catalog_events = [
            create_catalog_event_for_type(catalog_type, log_ts, catalog_data) for catalog_data in catalog_datas]
        logger.info("Queueing %s events for %s...", len(catalog_events), catalog_type.value)
        driver.queue_events_for_flush(EventCollection(catalog_events=catalog_events))
        logger.info("Flushing events for %s...", catalog_type.value)
        driver.flush_events(log_ts)
        logger.info("Flushed events for %s!", catalog_type.value)

    logger.info("Done")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise RuntimeError("Usage: db_inspect.py <yaml_config_filename>")

    config_filename = sys.argv[1]  # e.g. conf/example.yaml
    global_conf.load_from_yaml(config_filename)

    with create_db_session() as db:
        driver_meta = load_driver_meta_from_db(db, global_conf.organisation, global_conf.project)
        users = db.query(SyntheticUserSchema).filter_by(driver_meta_id=driver_meta.id)
        catalog_entries = db.query(CatalogEntrySchema).filter_by(driver_meta_id=driver_meta.id)

        embed()
