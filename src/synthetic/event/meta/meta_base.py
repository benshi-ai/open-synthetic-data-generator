from datetime import datetime
from typing import Optional

from synthetic.event.base import Event


class MetaEvent(Event):
    def __init__(self, user: "SyntheticUser", ts: datetime):  # type: ignore
        super().__init__(ts)

        self.user = user

    def __str__(self):
        return "Meta: %s - %s" % (
            self.ts,
            self.user.get_platform_uuid(),
        )

    def perform_actions(self) -> Optional["EventCollection"]:  # type: ignore
        raise NotImplementedError()
