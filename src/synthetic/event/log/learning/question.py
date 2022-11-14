import logging
from datetime import datetime
from enum import Enum
from typing import List

from synthetic.constants import BlockType, CatalogType
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.catalog_utils import data_as_catalog_event

logger = logging.getLogger(__name__)


# logger.setLevel(logging.DEBUG)


class QuestionAction(Enum):
    ANSWER = "answer"
    SKIP = "skip"


class QuestionEvent(LogEvent):
    def __init__(
        self, user: SyntheticUser, ts: datetime, question_id: str, exam_id: str, action: QuestionAction, answer_id: str
    ):
        super().__init__(
            user,
            ts,
            True,
            "question",
            {"id": question_id, "exam_id": exam_id, "action": action.value, "answer_id": answer_id},
            block=BlockType.ELEARNING,
        )

        self._question_id = question_id

    def __str__(self):
        return "%s - %s: %s (%s), %s" % (
            self.ts,
            self.user.get_platform_uuid(),
            self.event_type,
            self.online,
            self.props["action"],
        )

    def get_schema_path(self) -> str:
        return "events/question"

    def generate_associated_catalog_events(self) -> List[CatalogEvent]:
        return [data_as_catalog_event(CatalogType.QUESTION, self._question_id, self.ts)]
