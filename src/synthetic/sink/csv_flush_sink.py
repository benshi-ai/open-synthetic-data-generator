import logging
import os
from typing import List, Type

from synthetic.conf import global_conf
from synthetic.event.log.nudge.nudge_response import NudgeResponseEvent
from synthetic.sink.flush_sink import FlushSink
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.log_base import LogEvent
from synthetic.utils.file import write_log_events_to_csv, write_catalog_events_to_csv

logger = logging.getLogger(__name__)


def filter_log_events(log_events: List[LogEvent]) -> List[LogEvent]:
    filtered_log_events: List[LogEvent] = []
    allowed_log_event_types: List[Type[LogEvent]] = []

    if len(allowed_log_event_types) == 0:
        return filtered_log_events

    for log_event in log_events:
        allowed_type = any([isinstance(log_event, log_event_type) for log_event_type in allowed_log_event_types])
        if not allowed_type:
            continue

        if isinstance(log_event, NudgeResponseEvent) and log_event.props["nudge_id"] < 0:
            continue

        filtered_log_events.append(log_event)

    return filtered_log_events


def filter_catalog_events(catalog_events: List[CatalogEvent]) -> List[CatalogEvent]:
    filtered_catalog_events: List[CatalogEvent] = []
    allowed_catalog_event_types: List[Type[CatalogEvent]] = []

    if len(allowed_catalog_event_types) == 0:
        return filtered_catalog_events

    for catalog_event in catalog_events:
        allowed_type = any(
            [isinstance(catalog_event, catalog_event_type) for catalog_event_type in allowed_catalog_event_types]
        )
        if not allowed_type:
            continue

        filtered_catalog_events.append(catalog_event)

    return filtered_catalog_events


class CSVFlushSink(FlushSink):
    def flush_log_events(self, log_events: List[LogEvent]):
        if global_conf.log_events_filename is None:
            raise ValueError("No log filename configured!")

        output_dirname = os.path.dirname(global_conf.log_events_filename)
        if not os.path.exists(output_dirname):
            os.makedirs(output_dirname)

        if global_conf.filter_log_events_for_csv:
            written_log_events = filter_log_events(log_events)
        else:
            written_log_events = log_events

        write_log_events_to_csv(written_log_events, global_conf.log_events_filename)

    def flush_catalog_events(self, catalog_events: List[CatalogEvent]):
        if global_conf.catalog_events_filename is None:
            raise ValueError("No catalog filename configured!")

        output_dirname = os.path.dirname(global_conf.catalog_events_filename)
        if not os.path.exists(output_dirname):
            os.makedirs(output_dirname)

        if global_conf.filter_log_events_for_csv:
            written_catalog_events = filter_catalog_events(catalog_events)
        else:
            written_catalog_events = catalog_events

        write_catalog_events_to_csv(written_catalog_events, global_conf.catalog_events_filename)
