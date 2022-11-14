import mock
from datetime import datetime

from mock.mock import Mock
from synthetic.utils.nudge_utils import get_nudges_from_backend
from synthetic.utils.time_utils import datetime_to_payload_str


@mock.patch("synthetic.sink.http_flush_sink.requests.get")
def test_get_nudges(m_requests_get):
    response = Mock(
        status_code=200,
        json=lambda: [
            {
                "action": {
                    "created_at": "ea cupidatat",
                    "created_by": "quis sint",
                    "definition": {},
                    "description": "nulla dolor",
                    "id": 33690031,
                    "name": "amet cupidatat do in",
                    "org_proj": "eu",
                    "tags": ["Ut in Duis sed", "qui occaecat"],
                    "target_cohort_ids": [-9458228, 49202241],
                    "type": "ut ad in",
                },
                "queued_at": datetime_to_payload_str(datetime(2001, 1, 1, 1, 1, 1)),
                "id": -97914866,
            },
            {
                "action": {
                    "created_at": "irure enim culpa officia",
                    "created_by": "Duis",
                    "definition": {},
                    "description": "ad tempor in",
                    "id": -26796961,
                    "name": "labore sed ad occaecat",
                    "org_proj": "voluptate tempor nisi cillum",
                    "tags": ["Duis", "officia eiusmod Duis"],
                    "target_cohort_ids": [47237390, -86311975],
                    "type": "aliqua",
                },
                "queued_at": datetime_to_payload_str(datetime(2002, 2, 2, 2, 2, 2)),
                "id": -32731650,
            },
        ],
    )

    m_requests_get.return_value = response

    api_url = "http://www.abc.com"
    api_key = "super_secret_key"
    subject_id = "user_123"
    dispatched_at = datetime(2001, 1, 1, 12, 0, 0)
    nudges = get_nudges_from_backend(api_url, api_key, subject_id, dispatched_at)

    m_requests_get.assert_called_with(
        url=f"{api_url}/nudge/sdk/get",
        params={'subject_id': subject_id, 'queued_at': datetime_to_payload_str(dispatched_at)},
        headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {api_key}'},
    )

    assert [nudge.nudge_id for nudge in nudges] == [-97914866, -32731650]
    assert [nudge.subject_id for nudge in nudges] == ['user_123', 'user_123']
    assert [nudge.queued_at for nudge in nudges] == [datetime(2001, 1, 1, 1, 1, 1), datetime(2002, 2, 2, 2, 2, 2)]
