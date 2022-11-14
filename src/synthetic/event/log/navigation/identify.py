from datetime import datetime
from enum import Enum
from typing import Dict, Any

from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser

SEND_USER_DATA_IN_LOGS = False


class IdentifyAction(Enum):
    REGISTER = "register"
    LOGIN = "login"
    LOGOUT = "logout"


def build_props_for_action(user: SyntheticUser, event_type: IdentifyAction) -> Dict:
    props: Dict[str, Any] = {"action": event_type.value}
    if SEND_USER_DATA_IN_LOGS and event_type in [IdentifyAction.REGISTER, IdentifyAction.LOGIN]:
        props["user_props"] = user.get_logged_user_data()

    return props


class IdentifyEvent(LogEvent):
    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        action: IdentifyAction,
    ):
        super().__init__(user, ts, online, "identify", props=build_props_for_action(user, action))

    def get_schema_path(self) -> str:
        return "events/identify"
