from datetime import datetime
from typing import List

from synthetic.constants import BlockType
from synthetic.event.log.commerce.constants import ItemObject
from synthetic.event.log.log_base import LogEvent
from synthetic.user.synthetic_user import SyntheticUser


class SearchEvent(LogEvent):
    def __init__(
        self,
        user: SyntheticUser,
        ts: datetime,
        online: bool,
        search_id: str,
        query: str,
        results_list: List[ItemObject],
        page_number: int = 1,
        block: BlockType = BlockType.CORE,
    ):
        super().__init__(
            user,
            ts,
            online,
            "search",
            {
                "id": search_id,
                "query": query,
                "results_list": [result.get_payload_dict() for result in results_list],
                "page": page_number,
            },
            block=block,
        )

    def get_schema_path(self) -> str:
        return "events/search"
