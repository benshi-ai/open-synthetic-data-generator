from datetime import datetime
from enum import Enum

from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser


class AppAction(Enum):
    OPEN = "open"
    CLOSE = "close"
    BACKGROUND = "background"
    RESUME = "resume"


class AppEvent(LogEvent):
    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        action: AppAction,
    ):
        super().__init__(user, ts, online, "app", props={"action": action.value})

    def get_schema_path(self) -> str:
        return "events/app"
