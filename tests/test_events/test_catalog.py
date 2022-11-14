from datetime import datetime

from synthetic.catalog.generator import create_random_catalog_events_for_type
from synthetic.constants import CatalogType
from synthetic.utils.test_utils import assert_dicts_equal_partial


def test_media_catalog(fixed_seed):
    ts = datetime(2001, 1, 1)
    media_catalog_events = create_random_catalog_events_for_type(CatalogType.MEDIA_VIDEO, ts)

    assert len(media_catalog_events) == 1
    event = media_catalog_events[0]
    assert_dicts_equal_partial(
        media_catalog_events[0].as_payload_dict(),
        {
            'id': f'video_{event._data["uuid"]}',
            'id_source': event._data["uuid"],
            'lang': 'en',
        },
    )
