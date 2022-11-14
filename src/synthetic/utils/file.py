import csv
import os

from typing import List

from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.log_base import LogEvent


def write_log_events_to_csv(events: List[LogEvent], output_filename: str):
    if len(events) == 0:
        return

    event_dicts = [event.as_csv_dict() for event in events]
    try:
        if not os.path.isfile(output_filename):
            with open(output_filename, 'w') as f:
                writer = csv.DictWriter(f, list(event_dicts[0].keys()))
                writer.writeheader()
                for event in event_dicts:
                    # write a row to the csv file
                    writer.writerow(event)
        else:
            with open(output_filename, 'a') as f:
                # create the csv writer
                writer = csv.DictWriter(f, list(event_dicts[0].keys()))

                for event in event_dicts:
                    # write a row to the csv file
                    writer.writerow(event)
    except Exception as e:
        print(e)
        raise e


def write_catalog_events_to_csv(events: List[CatalogEvent], output_filename: str):
    if len(events) == 0:
        return

    event_dicts = [event.as_csv_dict() for event in events]
    try:
        if not os.path.isfile(output_filename):
            with open(output_filename, 'w') as f:
                writer = csv.DictWriter(f, list(event_dicts[0].keys()))
                writer.writeheader()
                for event in event_dicts:
                    # write a row to the csv file
                    writer.writerow(event)
        else:
            with open(output_filename, 'a') as f:
                # create the csv writer
                writer = csv.DictWriter(f, list(event_dicts[0].keys()))

                for event in event_dicts:
                    # write a row to the csv file
                    writer.writerow(event)
    except Exception as e:
        print(e)
        raise e
