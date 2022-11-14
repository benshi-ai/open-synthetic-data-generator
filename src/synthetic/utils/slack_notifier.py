import json
import logging
import sys
import requests

from copy import deepcopy
from enum import Enum
from typing import Dict, List, Tuple, Optional, Any

logger = logging.getLogger(__name__)


class MessageType(Enum):
    WARNING = ("Warning",)
    ERROR = ("Error",)
    INFO = ("Info",)


WARNING_COLOR = "#FAE705"
ERROR_COLOR = "#FA3005"
INFORMATION_COLOR = "#050CFA"

MAX_EXCEPTION_STRING_LENGTH = 128


def truncate_block(block: Dict, max_char_count_per_block=3000) -> Dict:
    for key in block:
        value = block[key]
        if key == "text":
            if isinstance(value, dict):
                block[key] = truncate_block(value)
            elif len(value) > max_char_count_per_block:
                block[key] = f"{value[0:max_char_count_per_block - 3]}..."

    return block


def truncate_blocks(blocks: List[Dict], max_block_count: int = 50, max_char_count_per_block: int = 3000) -> List[Dict]:
    if len(blocks) > max_block_count:
        blocks = blocks[0 : max_block_count - 1]
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "Message truncated..."}})

    truncated_blocks = [
        truncate_block(deepcopy(block), max_char_count_per_block=max_char_count_per_block) for block in blocks
    ]

    return truncated_blocks


def get_emoji_and_color_for_message_type(message_type: MessageType) -> Tuple[str, str]:
    if message_type == MessageType.WARNING:
        emoji = ":warning:"
        color = WARNING_COLOR
    elif message_type == MessageType.ERROR:
        emoji = ":fire:"
        color = ERROR_COLOR
    elif message_type == message_type.INFO:
        emoji = ":information_source:"
        color = INFORMATION_COLOR
    else:
        raise ValueError("Invalid message type: %s" % (message_type,))

    return emoji, color


class Slack:
    channel_urls = {
        "#synthetic-data-alerts": "https://hooks.slack.com/services/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",
    }
    sender = "pipeline-monitor"

    @classmethod
    def notify_payload(cls, channel_name: str, payload: Dict):
        byte_length = str(sys.getsizeof(payload))
        headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
        response = requests.post(cls.channel_urls[channel_name], data=json.dumps(payload), headers=headers)
        if response.status_code != 200:
            raise RuntimeError(
                f"Slack failed to send notification and returned code = {response.status_code}", response.text
            )

    @classmethod
    def notify_simple(cls, title: str, message: str, message_type: MessageType, channel_name: Optional[str] = None):
        emoji, color = get_emoji_and_color_for_message_type(message_type)
        if channel_name is None:
            channel_name = "#synthetic-data-alerts"

        slack_data = {
            "username": cls.sender,
            "icon_emoji": emoji,
            "attachments": [
                {
                    "fields": [
                        {
                            "title": emoji + " " + title,
                            "value": message,
                            "short": "false",
                        }
                    ],
                }
            ],
        }

        Slack.notify_payload(channel_name, slack_data)

    @classmethod
    def notify_exception(cls, exc: Exception, org_proj: str, channel_name: Optional[str] = None):
        emoji = ":fire:"
        if channel_name is None:
            channel_name = "#synthetic-data-alerts"

        exception_string = str(exc)
        if len(exception_string) > MAX_EXCEPTION_STRING_LENGTH:
            exception_string = exception_string[0 : MAX_EXCEPTION_STRING_LENGTH - 3] + "..."

        error_message = f"{emoji} Synthetic data generator failure on {org_proj}: {exception_string}"

        error_blocks: List[Dict] = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": error_message,
                },
            }
        ]

        slack_data = {
            "username": cls.sender,
            "icon_emoji": emoji,
            "text": error_message,
            "blocks": error_blocks,
        }

        Slack.notify_payload(channel_name, slack_data)

    @classmethod
    def notify_blocks(cls, blocks: List[Dict], channel_name: str = "#pipeline-alerts"):
        # blocks = truncate_blocks(blocks)

        slack_data = {"username": cls.sender, "blocks": blocks}

        Slack.notify_payload(channel_name, slack_data)


def slack_header_block(text, _type="plain_text"):
    return {
        "type": "header",
        "text": {
            "type": _type,
            "text": text,
        },
    }


def slack_body_block(text, _type="mrkdwn", code_block: bool = False):
    return {"type": "section", "text": {"type": _type, "text": text if not code_block else f"```{text}```"}}


def slack_divider_block():
    return {"type": "divider"}


def slack_run_url_notify_block(run_url: str) -> Dict[str, Any]:
    return {
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "emoji": True, "text": "View Run"},
                "style": "primary",
                "url": run_url,
            }
        ],
    }
