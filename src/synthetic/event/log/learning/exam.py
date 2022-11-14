import logging
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List

from synthetic.constants import BlockType, CatalogType
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.catalog_utils import data_as_catalog_event

logger = logging.getLogger(__name__)


# logger.setLevel(logging.DEBUG)


class ExamAction(Enum):
    START = "start"
    SUBMIT = "submit"
    RESULT = "result"


def build_props_for_action(
    exam_id: str, action: ExamAction, duration: Optional[int], score: Optional[float], is_passed: Optional[bool]
):
    result: Dict[str, Any] = {"id": exam_id, "action": action.value}
    if action == ExamAction.SUBMIT:
        assert duration is not None
        result["duration"] = duration
    elif action == ExamAction.RESULT:
        assert score is not None
        assert is_passed is not None
        result["score"] = score
        result["is_passed"] = is_passed

    return result


class ExamEvent(LogEvent):
    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        exam_id: str,
        action: ExamAction,
        duration: Optional[int] = None,  # for submit
        score: Optional[float] = None,  # for result
        is_passed: Optional[bool] = None,  # for result
    ):
        super().__init__(
            user,
            ts,
            online,
            "exam",
            build_props_for_action(exam_id, action, duration, score, is_passed),
            block=BlockType.ELEARNING,
        )

        self._exam_id = exam_id

    def __str__(self):
        return "%s - %s: %s (%s), %s" % (
            self.ts,
            self.user.get_platform_uuid(),
            self.event_type,
            self.online,
            self.props["action"],
        )

    def get_schema_path(self) -> str:
        return "events/exam"

    def generate_associated_catalog_events(self) -> List[CatalogEvent]:
        return [data_as_catalog_event(CatalogType.EXAM, self._exam_id, self.ts)]
