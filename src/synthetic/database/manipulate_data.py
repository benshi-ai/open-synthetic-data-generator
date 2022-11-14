import logging
import sys

from datetime import timedelta
from synthetic.conf import global_conf
from synthetic.database.schemas import SyntheticUserSchema, DriverMetaSchema
from synthetic.utils.database import create_db_session
from synthetic.utils.current_time_utils import get_current_time

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise RuntimeError("Usage: manipulate_data.py <yaml_config_filename>")

    config_filename = sys.argv[1]  # e.g. conf/example.yaml
    global_conf.load_from_yaml(config_filename)

    with create_db_session(global_conf.db_uri) as db_session:
        users = db_session.query(SyntheticUserSchema)
        for user in users:
            user.last_seen_ts = None
        db_session.commit()

        driver_metas = db_session.query(DriverMetaSchema)
        for driver_meta in driver_metas:
            driver_meta.last_seen_ts = get_current_time() - timedelta(days=1)
        db_session.commit()
