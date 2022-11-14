import logging
import random
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from synthetic.conf import global_conf
from synthetic.database.db_cache import DatabaseCache
from synthetic.database.db_session_wrapper import DBSessionWrapper
from synthetic.database.schemas import CatalogEntrySchema
from synthetic.constants import CatalogType
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.log.commerce.constants import ItemType

from synthetic.utils.database import (
    populate_catalog_to_count_in_db,
)
from synthetic.utils.random import select_random_keys_from_dict

logger = logging.getLogger(__name__)


def clean_promo_catalogs(current_ts: datetime):
    all_catalog_data = CatalogCache.cached_catalog[CatalogType.PROMO]
    for uuid, catalog_data in all_catalog_data.copy().items():
        if catalog_data['end_timestamp'] < current_ts.timestamp():
            del all_catalog_data[uuid]


def clean_catalogs_in_db(db_session: DBSessionWrapper, driver_meta_id: int):
    updated_entries: List[CatalogEntrySchema] = []
    all_catalog_data_list = (
        db_session.query(CatalogEntrySchema).filter_by(driver_meta_id=driver_meta_id, type="ecommerce_shop_item").all()
    )

    for entry in all_catalog_data_list:
        entry.type = CatalogType.DRUG.value
        updated_entries.append(entry)

    if len(updated_entries) > 0:
        for entry in updated_entries:
            db_session.add(entry)
        db_session.commit()

        logger.critical("Fixed %s catalog entries in DB!", len(updated_entries))


class CatalogCache:
    """Caches catalog data from the db and generates/persists new data as required"""

    cached_catalog: Dict[CatalogType, Dict[str, Any]] = {}

    # Map mapping item type to a dictionary of item uuids to applicable promotion ids with their cost ratio
    current_promotions: Dict[ItemType, Dict[str, List[Tuple[str, float]]]] = {}

    @staticmethod
    def clear():
        CatalogCache.cached_catalog = {}
        CatalogCache.current_promotions = {}

    @staticmethod
    def warm_up_for(
        catalog_type: CatalogType,
        db_session: DBSessionWrapper,
        driver_meta_id: int,
        target_catalog_count: int,
        current_ts: datetime,
    ) -> List[CatalogEvent]:
        assert isinstance(driver_meta_id, int)
        from synthetic.catalog.generator import logger, postprocess_catalog_data

        logger.debug("Populating cache for %s...", catalog_type)
        new_catalog_events = populate_catalog_to_count_in_db(
            db_session=db_session,
            driver_meta_id=driver_meta_id,
            catalog_type=catalog_type,
            target_count=target_catalog_count,
            current_ts=current_ts,
        )

        all_catalog_data = db_session.query(CatalogEntrySchema).filter_by(
            driver_meta_id=driver_meta_id, type=catalog_type.value
        )

        all_catalog_data_list: List[CatalogEntrySchema] = all_catalog_data.all()

        CatalogCache.cached_catalog[catalog_type] = dict(
            [(data.platform_uuid, postprocess_catalog_data(data.data)) for data in all_catalog_data_list]
        )

        if catalog_type == CatalogType.PROMO:
            clean_promo_catalogs(current_ts)

        return new_catalog_events

    @staticmethod
    def warm_up(
        db_session: DBSessionWrapper,
        driver_meta_id: Optional[int] = None,
        current_ts: datetime = None,
    ) -> Dict[CatalogType, List[CatalogEvent]]:
        if driver_meta_id is None:
            driver_meta_data = DatabaseCache.get_driver_meta(
                global_conf.organisation, global_conf.project, db_session=db_session
            )
            assert driver_meta_data is not None
            driver_meta_id = driver_meta_data["id"]

        if driver_meta_id is None:
            raise ValueError("No driver meta found!")

        if current_ts is None:
            current_ts = global_conf.start_ts

        new_catalogs = {}
        CatalogCache.clear()

        clean_catalogs_in_db(db_session, driver_meta_id)

        for catalog_type in CatalogType:
            if catalog_type == CatalogType.USER:
                # We don't do users
                continue

            catalog_config = global_conf.get_catalog_config(catalog_type)
            if catalog_config is None:
                continue

            new_catalogs[catalog_type] = CatalogCache.warm_up_for(
                catalog_type=catalog_type,
                db_session=db_session,
                driver_meta_id=driver_meta_id,
                target_catalog_count=catalog_config.target_count,
                current_ts=current_ts,
            )

        return new_catalogs

    @staticmethod
    def get_random_unique_catalogs(count: int) -> List[Tuple[CatalogType, Dict[str, Any]]]:
        if count == 0:
            return []

        all_catalogs: List[Tuple[CatalogType, Dict[str, Any]]] = []
        for item_type in list(ItemType):
            catalog_type = CatalogType(item_type.value)
            all_catalogs.extend([(catalog_type, value) for value in CatalogCache.cached_catalog[catalog_type].values()])

        return random.sample(all_catalogs, k=count)

    @staticmethod
    def get_random_unique_catalogs_for_type(catalog_type: CatalogType, count: int) -> List[Dict[str, Any]]:
        count = min(count, len(CatalogCache.cached_catalog[catalog_type]))

        if count <= 0:
            return []

        return random.sample(list(CatalogCache.cached_catalog[catalog_type].values()), k=count)

    @staticmethod
    def get_random_unique_catalogs_from_distribution(
        catalog_probabilities: Dict[CatalogType, float], count: int
    ) -> List[Tuple[CatalogType, Dict[str, Any]]]:
        catalog_counts: Dict[CatalogType, int] = {}
        for offset in range(0, count):
            catalog_type: CatalogType = select_random_keys_from_dict(catalog_probabilities, count=1)[0]
            if catalog_type not in catalog_counts:
                catalog_counts[catalog_type] = 0

            catalog_counts[catalog_type] += 1

        for catalog_type in catalog_counts:
            catalog_counts[catalog_type] = min(
                catalog_counts[catalog_type], len(CatalogCache.cached_catalog[catalog_type])
            )

        catalogs: List[Tuple[CatalogType, Dict[str, Any]]] = []
        for catalog_type, catalog_count in catalog_counts.items():
            catalogs_of_type = CatalogCache.get_random_unique_catalogs_for_type(catalog_type, count=catalog_count)
            catalogs.extend([(catalog_type, catalog) for catalog in catalogs_of_type])

        return catalogs

    @staticmethod
    def get_random_catalogs(catalog_type: CatalogType, count: int) -> List[Dict[str, Any]]:
        if count == 0:
            return []

        return random.choices(list(CatalogCache.cached_catalog[catalog_type].values()), k=count)

    @staticmethod
    def get_random_catalog_of_type(catalog_type: CatalogType) -> Dict[str, Any]:
        catalog_options = list(CatalogCache.cached_catalog[catalog_type].values())
        assert len(catalog_options) > 0, "No catalogs for %s!" % (catalog_type.value,)
        return random.choice(catalog_options)

    @classmethod
    def get_random_catalogs_from_distribution(cls, catalog_probabilities: Dict[CatalogType, float]) -> Dict[str, Any]:
        catalog_type: CatalogType = select_random_keys_from_dict(catalog_probabilities, count=1)[0]
        return cls.get_random_catalog_of_type(catalog_type)

    @staticmethod
    def get_all_catalogs(catalog_type: CatalogType) -> List[Dict[str, Any]]:
        if catalog_type not in CatalogCache.cached_catalog:
            return []

        return list(CatalogCache.cached_catalog[catalog_type].values())

    @staticmethod
    def get_catalogs_by_properties(catalog_type: CatalogType, properties: Dict[str, Any]) -> List[Dict[str, Any]]:
        matching_catalogs = []
        for catalog_uuid, catalog in CatalogCache.cached_catalog[catalog_type].items():
            all_match = True
            for key, value in properties.items():
                if catalog.get(key, None) != value:
                    all_match = False

            if not all_match:
                continue

            matching_catalogs.append(catalog)

        return matching_catalogs

    @staticmethod
    def get_catalog_by_uuid(catalog_type: CatalogType, uuid: str) -> Dict[str, Any]:
        if catalog_type not in CatalogCache.cached_catalog:
            CatalogCache.cached_catalog[catalog_type] = {}

        if uuid not in CatalogCache.cached_catalog[catalog_type]:
            raise ValueError(
                "No %s meta found for %s!"
                % (
                    catalog_type.value,
                    uuid,
                )
            )

        return CatalogCache.cached_catalog[catalog_type][uuid]

    @staticmethod
    def add_catalog_for_uuid(catalog_type: CatalogType, uuid: str, catalog_data: Dict[str, Any]):
        if catalog_type not in CatalogCache.cached_catalog:
            CatalogCache.cached_catalog[catalog_type] = {}

        CatalogCache.cached_catalog[catalog_type][uuid] = catalog_data

    @staticmethod
    def update_current_promotions():
        current_promotions: Dict[ItemType, Dict[str, List[Tuple[str, float]]]] = {}
        for promotion in CatalogCache.get_all_catalogs(CatalogType.PROMO):
            promotion_uuid = promotion['uuid']
            cost_adjustment_ratio = promotion['cost_adjustment_ratio']
            promoted_item_uuids = promotion['promoted_item_uuids']
            promoted_item_types_strings = promotion['promoted_item_types']
            promoted_item_types = [ItemType(item_type_str) for item_type_str in promoted_item_types_strings]
            for promoted_item_uuid, promoted_item_type in zip(promoted_item_uuids, promoted_item_types):
                if promoted_item_type not in current_promotions:
                    current_promotions[promoted_item_type] = {}

                if promoted_item_uuid not in current_promotions[promoted_item_type]:
                    current_promotions[promoted_item_type][promoted_item_uuid] = []

                current_promotions[promoted_item_type][promoted_item_uuid].append(
                    (promotion_uuid, cost_adjustment_ratio)
                )

        CatalogCache.current_promotions = current_promotions
