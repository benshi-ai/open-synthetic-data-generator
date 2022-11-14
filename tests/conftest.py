import os
import random
import uuid

import pytest
from sqlalchemy import create_engine

from synthetic.conf import global_conf, reset_configuration
from synthetic.database.db_cache import DatabaseCache
from synthetic.database.schemas import Base, DriverMetaSchema
from synthetic.utils.database import create_db_session
from synthetic.catalog.cache import CatalogCache


@pytest.fixture()
def fixed_seed():
    random.seed(0)

    old_uuid4 = uuid.uuid4
    rd = random.Random()
    rd.seed(0)
    uuid.uuid4 = lambda: uuid.UUID(int=rd.getrandbits(128))

    yield

    uuid.uuid4 = old_uuid4


@pytest.fixture(autouse=True)
def clear_cache():
    CatalogCache.clear()
    DatabaseCache.clear()


@pytest.fixture(autouse=True)
def global_config(temp_dir):
    reset_configuration()

    global_conf.organisation = "demo_org"
    global_conf.project = "demo_proj"

    db_filename = os.path.join(temp_dir, "test-db.sqlite")
    if os.path.exists(db_filename):
        os.remove(db_filename)

    global_conf.db_uri = "sqlite:///%s" % (os.path.abspath(db_filename),)

    engine = create_engine(global_conf.db_uri)
    Base.metadata.create_all(engine)


@pytest.fixture()
def db_session(global_config):
    db_session = create_db_session()

    yield db_session

    db_session.close()


@pytest.fixture()
def driver_meta(db_session):
    driver_meta = DriverMetaSchema(organisation=global_conf.organisation, project=global_conf.project)
    db_session.add(driver_meta)
    db_session.commit()

    return driver_meta


@pytest.fixture()
def temp_dir():
    temp_dirname = os.path.join(os.getcwd(), "temp")
    if not os.path.exists(temp_dirname):
        os.makedirs(temp_dirname)

    return temp_dirname
