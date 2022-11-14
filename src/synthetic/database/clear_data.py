import logging
import os
import sys

from synthetic.conf import global_conf
from synthetic.utils.database import clear_db_data, create_db_session

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise RuntimeError("Usage: clear.py <yaml_config_filename>")

    config_filename = sys.argv[1]  # e.g. conf/example.yaml
    global_conf.load_from_yaml(config_filename)

    with create_db_session(global_conf.db_uri) as db_session:
        clear_db_data(db_session)
        logger.info("Cleared DB data.")

    if global_conf.log_events_filename is not None and os.path.exists(global_conf.log_events_filename):
        os.remove(global_conf.log_events_filename)
        logger.info("Removed %s.", global_conf.log_events_filename)
    if global_conf.catalog_events_filename is not None and os.path.exists(global_conf.catalog_events_filename):
        os.remove(global_conf.catalog_events_filename)
        logger.info("Removed %s.", global_conf.catalog_events_filename)
