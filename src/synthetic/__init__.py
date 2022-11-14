import logging
import os

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

DATA_DIRNAME = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
TEST_DATA_DIRNAME = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "tests", "data"
)

PREDEFINED_CATALOG_DIRNAME = os.path.join(DATA_DIRNAME, "catalog")
