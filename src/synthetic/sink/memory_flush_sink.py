from typing import List

from synthetic.sink.flush_sink import FlushSink
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.log_base import LogEvent


class MemoryFlushSink(FlushSink):
    def __init__(self):
        super().__init__()
        self.flushed_logs: List[LogEvent] = []
        self.flushed_catalogs: List[CatalogEvent] = []

    def clear_all(self):
        self.flushed_logs = []
        self.flushed_catalogs = []

    def flush_log_events(self, log_events: List[LogEvent]):
        self.flushed_logs.extend(log_events)

    def flush_catalog_events(self, catalog_events: List[CatalogEvent]):
        self.flushed_catalogs.extend(catalog_events)
