import os
import logging
import sys
from logging.handlers import RotatingFileHandler

from synthetic.conf import global_conf
from synthetic.driver.driver import Driver

PROFILE = False

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise RuntimeError("Usage: synthetic_data.py <yaml_config_filename>")

    config_filename = sys.argv[1]  # e.g. conf/example.yaml
    global_conf.load_from_yaml(config_filename)

    logger = logging.getLogger()
    logs_dirname = os.path.join(os.path.dirname(__file__), "logs")
    if not os.path.exists(logs_dirname):
        os.makedirs(logs_dirname)

    handler = RotatingFileHandler(
        os.path.join(logs_dirname, '%s_%s.log' % (global_conf.organisation, global_conf.project)),
        maxBytes=5000000,
        backupCount=10,
    )
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

    driver = Driver(global_conf.sink_types)

    if PROFILE:
        import cProfile

        cProfile.run("driver.run()")
    else:
        driver.run()
