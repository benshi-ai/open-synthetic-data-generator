import dataclasses
from dataclasses import field
from datetime import datetime
from typing import List, Optional

from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.log_base import LogEvent
from synthetic.event.meta.meta_base import MetaEvent


@dataclasses.dataclass
class EventCollection:
    catalog_events: List[CatalogEvent] = field(default_factory=lambda: [])
    log_events: List[LogEvent] = field(default_factory=lambda: [])
    meta_events: List[MetaEvent] = field(default_factory=lambda: [])

    def clear(self):
        self.catalog_events = []
        self.log_events = []
        self.meta_events = []

    def insert_log_event(self, event: LogEvent):
        self.log_events.append(event)
        self._sort_log_events()

    def insert_meta_event(self, event: MetaEvent):
        self.meta_events.append(event)
        self._sort_meta_events()

    def _sort_log_events(self):
        self.log_events = sorted(self.log_events, key=lambda event: event.ts, reverse=True)

    def _sort_catalog_events(self):
        self.catalog_events = sorted(self.catalog_events, key=lambda event: event.ts, reverse=True)

    def _sort_meta_events(self):
        self.meta_events = sorted(self.meta_events, key=lambda event: event.ts, reverse=True)

    def _sort_events(self):
        self._sort_log_events()
        self._sort_catalog_events()
        self._sort_meta_events()

    def insert_events(self, events: "EventCollection"):
        self.log_events.extend(events.log_events)
        self._sort_log_events()

        self.catalog_events.extend(events.catalog_events)
        self._sort_catalog_events()

        self.meta_events.extend(events.meta_events)
        self._sort_meta_events()

    def pop_events_before(self, end_ts: datetime) -> "EventCollection":
        generated_events = EventCollection()
        # We produce some log events from the scheduled ones
        while len(self.log_events) > 0 and self.log_events[-1].ts < end_ts:
            generated_log_event = self.log_events.pop()
            generated_events.log_events.append(generated_log_event)

        # We produce some catalog events from the scheduled ones
        while len(self.catalog_events) > 0 and self.catalog_events[-1].ts <= end_ts:
            generated_catalog_event = self.catalog_events.pop()
            generated_events.catalog_events.append(generated_catalog_event)

        # We produce some meta events from the scheduled ones
        while len(self.meta_events) > 0 and self.meta_events[-1].ts <= end_ts:
            generated_meta_event = self.meta_events.pop()
            generated_events.meta_events.append(generated_meta_event)

        generated_events._sort_events()

        return generated_events

    def is_empty(self):
        return len(self.catalog_events) == 0 and len(self.meta_events) == 0 and len(self.log_events) == 0

    def assert_integrity(self, current_ts: datetime):
        for log_event in self.log_events:
            assert log_event.ts <= current_ts, "Current ts: %s, future event: %s" % (
                current_ts,
                log_event,
            )
        for catalog_event in self.catalog_events:
            assert catalog_event.ts <= current_ts, "Current ts: %s, future event: %s" % (
                current_ts,
                catalog_event,
            )
        for meta_event in self.meta_events:
            assert meta_event.ts <= current_ts, "Current ts: %s, future event: %s" % (
                current_ts,
                meta_event,
            )

    def get_latest_ts(self) -> Optional[datetime]:
        max_ts: Optional[datetime] = None

        for log_event in self.log_events:
            max_ts = log_event.ts if max_ts is None else max(max_ts, log_event.ts)

        for catalog_event in self.catalog_events:
            max_ts = catalog_event.ts if max_ts is None else max(max_ts, catalog_event.ts)

        for meta_event in self.meta_events:
            max_ts = meta_event.ts if max_ts is None else max(max_ts, meta_event.ts)

        return max_ts
