import sys

from sqlalchemy import create_engine

from synthetic.conf import global_conf
from synthetic.database.schemas import Base

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise RuntimeError("Usage: synthetic_data.py <yaml_config_filename>")

    config_filename = sys.argv[1]  # e.g. conf/example.yaml
    global_conf.load_from_yaml(config_filename)

    db_engine = create_engine(global_conf.db_uri)
    Base.metadata.drop_all(db_engine)

    with db_engine.connect() as con:
        con.execute("drop table alembic_version")
