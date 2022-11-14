import logging
import json
import time
from typing import List, Dict, Optional, Union

import requests

from synthetic.constants import SUPPORTED_CATALOG_TYPES
from synthetic.conf import global_conf
from synthetic.event.constants import SubjectType
from synthetic.sink.flush_sink import FlushSink
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.log_base import LogEvent
from synthetic.utils.slack_notifier import Slack, MessageType

logger = logging.getLogger(__name__)

FAKE_CALLS = False
RATE_LIMITING_SLEEP_SECONDS = 0.01


def post_payload_with_retries(url: str, headers: Dict[str, str], payload: Union[List, Dict], retry_count=10):
    current_retry_wait = 2
    used_retries = 0
    if FAKE_CALLS:
        logger.critical("Called payload %s with %s", url, payload)
    else:
        serialised_payload = json.dumps(payload)

        # Initial try
        res = requests.post(url="%s" % (url,), data=serialised_payload, headers=headers)
        remaining_retries = retry_count
        while res.status_code != 200 and remaining_retries > 0:
            if global_conf.notify and remaining_retries < 5:
                Slack.notify_simple(
                    "Error when posting to backend (%s): %s"
                    % (
                        res.status_code,
                        url,
                    ),
                    message=res.text,
                    message_type=MessageType.ERROR,
                )

            logger.critical("Error when posting to %s: %s", url, res.text)
            logger.critical("Failed to send payload... waiting %s seconds before trying again!", current_retry_wait)
            time.sleep(current_retry_wait)
            remaining_retries -= 1
            current_retry_wait *= 2

            # Successive tries
            res = requests.post(url="%s" % (url,), data=serialised_payload, headers=headers)
            used_retries += 1

        if res.status_code != 200:
            # We still failed even after all the retries
            logger.critical("Error when sending payload: %s, %s", res.status_code, res.text)
            logger.critical("Detailed error: %s", res.json())
            raise RuntimeError(res.json())

    if used_retries > 0:
        logger.critical("Required %s retries when sending payload!", used_retries)


def get_data_with_retries(url: str, headers: Dict[str, str], params: Dict[str, str], retry_count=12) -> Optional[Dict]:
    current_retry_wait = 2
    used_retries = 0
    if FAKE_CALLS:
        logger.critical("Called get %s", url)
        return {}
    else:
        logger.debug("Fetching data from %s...", url)
        res = requests.get(
            url=url,
            params=params,
            headers=headers,
        )
        # Initial try
        remaining_retries = retry_count
        while res.status_code != 200 and remaining_retries > 0:
            if global_conf.notify:
                Slack.notify_simple(
                    "Error when getting from backend (%s): %s"
                    % (
                        res.status_code,
                        url,
                    ),
                    message=res.text,
                    message_type=MessageType.ERROR,
                )
            logger.critical("Error when getting from %s: %s", url, res.text)
            logger.critical("Failed to get data... waiting %s seconds before trying again!", current_retry_wait)
            time.sleep(current_retry_wait)
            remaining_retries -= 1
            current_retry_wait *= 2

            # Successive tries
            res = requests.get(
                url=url,
                params=params,
                headers=headers,
            )
            used_retries += 1

        if res.status_code != 200:
            # We still failed even after all the retries
            logger.critical("Error when getting data: %s, %s", res.status_code, res.text)
            logger.critical("Detailed error: %s", res.json())
            raise RuntimeError(res.json())
        else:
            if used_retries > 0:
                logger.critical("Required %s retries when getting data!", used_retries)

            return res.json()


def send_log_events(events: List[LogEvent]):
    api_url = global_conf.api_url
    api_key = global_conf.api_key

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}

    payload = {"data": [event.as_payload_dict() for event in events]}
    post_payload_with_retries(f"{api_url}/data/ingest/log", headers, payload)


def build_catalog_data_payload(events: List[CatalogEvent]) -> List[Dict]:
    data_payload = [event.get_backend_data() for event in events]

    return data_payload


def send_catalog_events(subject_type: SubjectType, events: List[CatalogEvent]):
    if len(events) == 0:
        return

    api_url = global_conf.api_url
    api_key = global_conf.api_key

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}

    payload = build_catalog_data_payload(events)

    post_payload_with_retries(f"{api_url}/data/ingest/catalog/{subject_type.value}", headers, payload)


class HTTPFlushSink(FlushSink):
    def flush_log_events(self, log_events: List[LogEvent], logs_per_batch=5000):
        for i in range(0, len(log_events), logs_per_batch):
            if i + logs_per_batch >= len(log_events):
                # This is the final batch, don't sleep
                send_log_events(log_events[i:])
            else:
                send_log_events(log_events[i : i + logs_per_batch])
                time.sleep(RATE_LIMITING_SLEEP_SECONDS)

    def flush_catalog_events(self, catalog_events: List[CatalogEvent], logs_per_batch=5000):
        if len(catalog_events) == 0:
            return

        current_subject_type = catalog_events[0].catalog_type
        current_subject_logs: List[CatalogEvent] = []

        for catalog_event in catalog_events:
            if catalog_event.catalog_type not in SUPPORTED_CATALOG_TYPES:
                continue

            if current_subject_type is None:
                current_subject_type = catalog_event.catalog_type
            elif current_subject_type != catalog_event.catalog_type or len(current_subject_logs) >= logs_per_batch:
                # We have a change of subject, flush current logs
                send_catalog_events(current_subject_type, current_subject_logs)
                current_subject_type = catalog_event.catalog_type
                current_subject_logs = []
                time.sleep(RATE_LIMITING_SLEEP_SECONDS)

            current_subject_logs.append(catalog_event)

        if len(current_subject_logs) > 0:
            send_catalog_events(current_subject_type, current_subject_logs)
