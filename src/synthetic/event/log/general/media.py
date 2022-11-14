import logging
from datetime import datetime
from enum import Enum
from typing import List

from synthetic.constants import BlockType
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.constants import MediaType
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.catalog_utils import media_as_catalog_event

logger = logging.getLogger(__name__)


# logger.setLevel(logging.DEBUG)


class MediaAction(Enum):
    PLAY = "play"
    PAUSE = "pause"
    SEEK = "seek"
    FINISH = "finish"
    IMPRESSION = "impression"


class MediaEvent(LogEvent):
    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        media_type: MediaType,
        media_uuid: str,
        action: MediaAction,
        time_offset: float,  # Seek time in milliseconds
        block: BlockType = BlockType.CORE,
    ):
        """

        :param user:
        :param ts:
        :param online:
        :param media_type: {"video", "audio"}
        :param media_uuid:
        :param action: MediaAction
        :param time_offset: seek time in milliseconds between 0 and full duration of video:
        """
        super().__init__(
            user,
            ts,
            online,
            "media",
            {
                "type": media_type.value,
                "id": f"{media_type.value}_{media_uuid}",
                "id_source": media_uuid,
                "action": action.value,
                "time": float(time_offset),
            },
            block=block,
        )

        self._media_type = media_type
        self._media_uuid = media_uuid

    def get_schema_path(self) -> str:
        return "events/media"

    def generate_associated_catalog_events(self) -> List[CatalogEvent]:
        return [media_as_catalog_event(self._media_type, self._media_uuid, self.ts)]
