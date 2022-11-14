from datetime import datetime

from synthetic.catalog.cache import CatalogCache
from synthetic.catalog.generator import create_catalog_event_for_type
from synthetic.constants import CatalogType, Currency
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.constants import MediaType
from synthetic.event.log.commerce.constants import ShopItem


def shop_item_as_catalog_event(item: ShopItem, ts: datetime) -> CatalogEvent:
    catalog_type = CatalogType[item.item_type.name]
    catalog_data = CatalogCache.get_catalog_by_uuid(catalog_type, item.id).copy()
    if isinstance(catalog_data['currency'], Currency):
        catalog_data['currency'] = catalog_data['currency'].value
    return create_catalog_event_for_type(catalog_type, ts=ts, data=catalog_data)


def media_as_catalog_event(media_type: MediaType, uuid: str, ts: datetime) -> CatalogEvent:
    if media_type == MediaType.VIDEO:
        catalog_type = CatalogType.MEDIA_VIDEO
    elif media_type == MediaType.AUDIO:
        catalog_type = CatalogType.MEDIA_AUDIO
    elif media_type == MediaType.IMAGE:
        catalog_type = CatalogType.MEDIA_IMAGE
    else:
        raise ValueError(media_type)

    catalog_data = CatalogCache.get_catalog_by_uuid(catalog_type, uuid).copy()
    catalog_data['media_type'] = catalog_data['media_type'].value
    return create_catalog_event_for_type(catalog_type, ts=ts, data=catalog_data)


def data_as_catalog_event(catalog_type: CatalogType, uuid: str, ts: datetime) -> CatalogEvent:
    catalog_data = CatalogCache.get_catalog_by_uuid(catalog_type, uuid)
    return create_catalog_event_for_type(catalog_type, ts=ts, data=catalog_data)
