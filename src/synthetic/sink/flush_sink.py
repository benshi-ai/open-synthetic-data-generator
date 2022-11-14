import logging
from typing import List

from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.log_base import LogEvent

logger = logging.getLogger(__name__)


# logger.setLevel(logging.DEBUG)


class FlushSink:
    """A generic sync working on the flush principal"""

    def __init__(self):
        super().__init__()

    def flush_log_events(self, log_events: List[LogEvent]):
        raise NotImplementedError()

    def flush_catalog_events(self, catalog_events: List[CatalogEvent]):
        raise NotImplementedError()
