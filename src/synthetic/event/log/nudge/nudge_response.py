from datetime import datetime
from typing import Dict

from synthetic.event.constants import NudgeResponseAction
from synthetic.event.log.log_base import LogEvent
from synthetic.utils.nudge_utils import Nudge


def build_props(nudge: Nudge, nudge_action: NudgeResponseAction) -> Dict:
    result = {
        "nudge_id": nudge.nudge_id,
        "type": nudge.nudge_type.value,
        "response": {"action": nudge_action.value},
    }

    if nudge.action is not None:
        result["resolved_action"] = nudge.action

    return result


class NudgeResponseEvent(LogEvent):
    def __init__(
        self,
        user: "SyntheticUser",  # type: ignore
        response_ts: datetime,
        online: bool,
        nudge: Nudge,
        nudge_response_action: NudgeResponseAction,
    ):
        super().__init__(
            user,
            response_ts,
            online,
            "nudge_response",
            props=build_props(nudge, nudge_response_action),
        )

    @property
    def nudge_action(self) -> NudgeResponseAction:
        return NudgeResponseAction(self.props["response"]["action"])

    def get_schema_path(self) -> str:
        return "events/nudge_response"
