from datetime import datetime
from typing import Optional, Any, Dict

from synthetic.constants import BlockType
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser


def build_props(prev_level: int, new_level: int, module_id: Optional[str]):
    result: Dict[str, Any] = {"prev_level": prev_level, "new_level": new_level}

    if module_id is not None:
        result["module_id"] = module_id

    return result


class LevelEvent(LogEvent):
    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        prev_level: int,
        new_level: int,
        module_id: Optional[str] = None,
    ):
        super().__init__(
            user, ts, online, "level", build_props(prev_level, new_level, module_id), block=BlockType.LOYALTY
        )

    def get_schema_path(self) -> str:
        return "events/level"
