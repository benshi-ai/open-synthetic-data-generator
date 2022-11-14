import csv
import os
import random
import uuid
import logging
from datetime import datetime

from random import randrange
from typing import Dict, Any, List, Optional

from synthetic import PREDEFINED_CATALOG_DIRNAME
from synthetic.conf import global_conf
from synthetic.constants import CatalogType, Currency
from synthetic.event.catalog.blood_catalog import BloodCatalogEvent
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.catalog.drug_catalog import DrugCatalogEvent
from synthetic.event.catalog.media_catalog import MediaCatalogEvent
from synthetic.event.catalog.medical_equipment_catalog import MedicalEquipmentCatalogEvent
from synthetic.event.catalog.oxygen_catalog import OxygenCatalogEvent
from synthetic.event.catalog.promo_catalog import PromoCatalogEvent
from synthetic.event.log.commerce.constants import ItemType
from synthetic.utils.random import get_random_int_in_range, get_random_float_in_range
from synthetic.utils.user_utils import fake
from synthetic.database.db_session_wrapper import DBSessionWrapper
from synthetic.database.schemas import CatalogEntrySchema, DriverMetaSchema
from synthetic.event.constants import MediaType

logger = logging.getLogger(__name__)


def create_random_price() -> float:
    return random.random() * 100


def create_random_module_data_for_module_type(module_type: str) -> Dict[str, Any]:
    module = {
        "uuid": str(uuid.uuid4()),
        "type": "module",
        "name": module_type,
        "price": create_random_price(),
        "currency": Currency.USD,
        "availability": True,
    }

    return module


def create_drug_data(
    drug_name: Optional[str] = None,
    market_id: Optional[str] = None,
    description: Optional[str] = None,
    supplier_name: Optional[str] = None,
    supplier_id: Optional[str] = None,
    producer: Optional[str] = None,
    packaging: Optional[str] = None,
    active_ingredients: Optional[str] = None,
    drug_form: Optional[str] = None,
    drug_strength: Optional[str] = None,
    atc_anatomical_group: Optional[str] = None,
    otc_or_ethical: Optional[str] = None,
):
    active_ingredients_list = (
        [ingredient.strip() for ingredient in active_ingredients.split(',')]
        if active_ingredients is not None
        else [fake.sentence()]
    )
    active_ingredients_list = [ingredient for ingredient in active_ingredients_list if len(ingredient) > 0]

    return {
        "uuid": str(uuid.uuid4()),
        "item_price": create_random_price(),
        "currency": Currency.USD,
        "drug_name": drug_name.strip() if drug_name is not None else fake.name(),
        "active_ingredients": active_ingredients_list,
        "drug_form": drug_form.strip() if drug_form is not None else random.choice(["Gel", "Infus"]),
        "drug_strength": drug_strength.strip() if drug_strength is not None else fake.sentence(),
        "atc_anatomical_group": atc_anatomical_group.strip() if atc_anatomical_group is not None else fake.sentence(),
        "packaging": packaging.strip() if packaging is not None else fake.sentence(),
        "producer": producer.strip() if producer is not None else fake.name(),
        "otc_or_ethical": otc_or_ethical.strip() if otc_or_ethical is not None else random.choice(["Ethical", "OTC"]),
        "market_id": market_id.strip() if market_id is not None else str(random.randint(1000, 100000)),
        "description": description.strip() if description is not None else fake.sentence(),
        "supplier_name": supplier_name.strip() if supplier_name is not None else fake.name(),
        "supplier_id": supplier_id.strip() if supplier_id is not None else str(random.randint(1000, 100000)),
    }


def create_blood_data(
    market_id: Optional[str] = None,
    blood_component: Optional[str] = None,
    blood_group: Optional[str] = None,
    packaging: Optional[str] = None,
    packaging_size: Optional[str] = None,
    packaging_units: Optional[str] = None,
    supplier_id: Optional[str] = None,
    supplier_name: Optional[str] = None,
):
    return {
        "uuid": str(uuid.uuid4()),
        "market_id": market_id.strip() if market_id is not None else str(random.randint(1000, 100000)),
        "blood_component": blood_component.strip()
        if blood_component is not None
        else random.choice(
            ["Platelets", "Cryoprecipitate", "Whole blood", "Fresh Frozen Plasma", "Packed Red Blood Cells", "Other"]
        ),
        "blood_group": blood_group.strip()
        if blood_group is not None
        else random.choice(["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-", "Unsure"]),
        "packaging": packaging.strip() if packaging is not None else "pint",
        "packaging_size": packaging_size.strip() if packaging_size is not None else "1",
        "packaging_units": packaging_units.strip() if packaging_units is not None else "pints",
        "supplier_id": supplier_id.strip() if supplier_id is not None else fake.sentence(),
        "supplier_name": supplier_name.strip() if supplier_name is not None else fake.sentence(),
        "item_price": create_random_price(),
        "currency": Currency.USD,
    }


def create_oxygen_data(
    market_id: Optional[str] = None,
    packaging: Optional[str] = None,
    packaging_size: Optional[str] = None,
    packaging_units: Optional[str] = None,
    supplier_id: Optional[str] = None,
    supplier_name: Optional[str] = None,
):
    return {
        "uuid": str(uuid.uuid4()),
        "market_id": market_id.strip() if market_id is not None else str(random.randint(1000, 100000)),
        "packaging": packaging.strip() if packaging is not None else "cylinder",
        "packaging_size": packaging_size.strip()
        if packaging_size is not None
        else str(random.choice([0.5, 1, 1.5, 1.6, 2, 2.5, 5, 6, 7, 7.5, 8, 9, 10])),
        "packaging_units": packaging_units.strip() if packaging_units is not None else "cubic_meters",
        "supplier_id": supplier_id.strip() if supplier_id is not None else fake.sentence(),
        "supplier_name": supplier_name.strip() if supplier_name is not None else fake.sentence(),
        "item_price": create_random_price(),
        "currency": Currency.USD,
    }


def create_medical_equipment_data(
    name: Optional[str] = None,
    description: Optional[str] = None,
    market_id: Optional[str] = None,
    supplier_id: Optional[str] = None,
    supplier_name: Optional[str] = None,
    producer: Optional[str] = None,
    packaging: Optional[str] = None,
    packaging_size: Optional[str] = None,
    packaging_units: Optional[str] = None,
    category: Optional[str] = None,
):
    random_name, random_category = random.choice(
        [
            ('Syringe & Needle', 'needles'),
            ('2 - Way Catheter', 'catheters'),
            ('Chromic Sutures', 'sutures'),
            ('Crepe Bandage', 'sutures'),
            ('Vicryl Sutures', 'sutures'),
            ('21G Needle', 'needles'),
            ('Non - Powdered Latex Glove', 'gloves'),
            ('Surgical Gloves', 'gloves'),
            ('Disposable Face Mask', 'masks'),
            ('Adhesive Plaster 1"', 'plasters'),
            ('Adhesive Plaster 2"', 'plasters'),
            ('Adhesive Plaster 4"', 'plasters'),
            ('Cannula 18g', 'cannula'),
            ('Cannula 20g', 'cannula'),
            ('Cannula 22g', 'cannula'),
            ('Cannula 24g', 'cannula'),
            ('Nurse Cap', 'caps'),
            ('Scalp Vein', 'cannula'),
        ]
    )

    return {
        "uuid": str(uuid.uuid4()),
        "name": name.strip() if name is not None else random_name,
        "description": description.strip() if description is not None else fake.sentence(),
        "market_id": market_id.strip() if market_id is not None else str(random.randint(1000, 100000)),
        "supplier_id": supplier_id.strip() if supplier_id is not None else fake.sentence(),
        "supplier_name": supplier_name.strip() if supplier_name is not None else fake.sentence(),
        "producer": producer.strip() if producer is not None else fake.sentence(),
        "packaging": packaging.strip() if packaging is not None else "carton",
        "packaging_size": packaging_size.strip()
        if packaging_size is not None
        else str(random.choice([1, 8, 20, 25, 50, 100])),
        "packaging_units": packaging_units.strip() if packaging_units is not None else "units",
        "category": category.strip() if category is not None else random_category,
        "item_price": create_random_price(),
        "currency": Currency.USD,
    }


def create_random_category_data_for_item_type(item_type: ItemType, **kwargs) -> Dict[str, Any]:
    category = {"type": item_type.value}

    if item_type == ItemType.DRUG:
        category.update(create_drug_data(**kwargs))
    elif item_type == ItemType.BLOOD:
        category.update(create_blood_data(**kwargs))
    elif item_type == ItemType.OXYGEN:
        category.update(create_oxygen_data(**kwargs))
    elif item_type == ItemType.MEDICAL_EQUIPMENT:
        category.update(create_medical_equipment_data(**kwargs))

    return category


def create_predefined_catalog_events_for_type(
    simulation_profile: str, catalog_type: CatalogType, ts: datetime
) -> List[CatalogEvent]:
    if not os.path.exists(PREDEFINED_CATALOG_DIRNAME):
        raise ValueError("Predefined catalog directory does not exist!")

    csv_filename = os.path.join(PREDEFINED_CATALOG_DIRNAME, simulation_profile, f"{catalog_type.value}.csv")
    if not os.path.exists(csv_filename):
        return []

    catalogs = []
    with open(csv_filename, "r") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter="|")
        headers = None
        for row_index, row in enumerate(csv_reader):
            if row_index == 0:
                headers = row
                continue

            assert headers is not None
            row_data = dict(zip(headers, row))
            catalog_data = create_random_category_data_for_item_type(ItemType(catalog_type.value), **row_data)

            catalogs.append(CatalogEvent(catalog_type, ts, catalog_data))

    return catalogs


def create_catalog_event_for_type(catalog_type: CatalogType, ts: datetime, data: Dict) -> CatalogEvent:
    if catalog_type == CatalogType.BLOOD:
        return BloodCatalogEvent(catalog_type, ts, data=data)
    elif catalog_type == CatalogType.DRUG:
        return DrugCatalogEvent(catalog_type, ts, data=data)
    elif catalog_type == CatalogType.OXYGEN:
        return OxygenCatalogEvent(catalog_type, ts, data=data)
    elif catalog_type == CatalogType.MEDICAL_EQUIPMENT:
        return MedicalEquipmentCatalogEvent(catalog_type, ts, data=data)
    elif (
        catalog_type == CatalogType.MEDIA_VIDEO
        or catalog_type == CatalogType.MEDIA_AUDIO
        or catalog_type == CatalogType.MEDIA_IMAGE
    ):
        return MediaCatalogEvent(catalog_type, ts, data)
    elif catalog_type == CatalogType.PROMO:
        return PromoCatalogEvent(catalog_type, ts, data=data)
    else:
        return CatalogEvent(catalog_type, ts, data)


def create_random_catalog_events_for_type(catalog_type: CatalogType, ts: datetime) -> List[CatalogEvent]:
    catalog_config = global_conf.get_catalog_config(catalog_type)

    if catalog_type in [CatalogType.QUESTION, CatalogType.APP, CatalogType.ORDER, CatalogType.PROMO]:
        return []
    elif catalog_type == CatalogType.PAGE:
        return [
            create_catalog_event_for_type(
                catalog_type,
                ts,
                {
                    "uuid": str(uuid.uuid4()),
                    "path": fake.url(),
                    "title": fake.sentence(),
                },
            )
        ]
    elif catalog_type == CatalogType.MEDIA_VIDEO:
        return [
            create_catalog_event_for_type(
                catalog_type,
                ts,
                {
                    "uuid": str(uuid.uuid4()),
                    "media_type": "video",
                    "name": fake.sentence(),
                    "description": fake.sentence(),
                    "lang": "en",
                    "length": float(randrange(10 * 1000, 2000 * 1000)),  # Milliseconds
                    "resolution": str(random.choice(["360", "480", "720", "1080"])),
                },
            )
        ]
    elif catalog_type == CatalogType.MEDIA_AUDIO:
        min_length_seconds = catalog_config.properties.get("length_min_seconds", 1800)
        max_length_seconds = catalog_config.properties.get("length_max_seconds", 43200)
        length_seconds = (
            random.randrange(min_length_seconds, max_length_seconds)
            if max_length_seconds > min_length_seconds
            else min_length_seconds
        )

        return [
            create_catalog_event_for_type(
                catalog_type,
                ts,
                {
                    "uuid": str(uuid.uuid4()),
                    "media_type": "audio",
                    "name": fake.sentence(),
                    "description": fake.sentence(),
                    "lang": "en",
                    "length": float(length_seconds * 1000),  # Milliseconds
                    "resolution": random.choice(["64", "96", "128"]),
                },
            )
        ]
    elif catalog_type == CatalogType.MEDIA_IMAGE:
        return [
            create_catalog_event_for_type(
                catalog_type,
                ts,
                {
                    "uuid": str(uuid.uuid4()),
                    "media_type": "image",
                    "name": fake.sentence(),
                    "description": fake.sentence(),
                    "lang": "en",
                    "length": float(randrange(10, 2000)),
                    "resolution": random.choice(["360", "480", "720", "1080"]),
                },
            )
        ]
    elif catalog_type in [CatalogType.DRUG, CatalogType.BLOOD, CatalogType.OXYGEN, CatalogType.MEDICAL_EQUIPMENT]:
        item_type = ItemType[catalog_type.name]
        return [
            create_catalog_event_for_type(catalog_type, ts, data=create_random_category_data_for_item_type(item_type))
        ]
    elif catalog_type == CatalogType.ELEARNING_SHOP_ITEM:
        return [
            create_catalog_event_for_type(catalog_type, ts, data=create_random_module_data_for_module_type("elearning"))
        ]
    elif catalog_type == CatalogType.MODULE:
        min_length_seconds = catalog_config.properties.get("length_min_seconds", 1800)
        max_length_seconds = catalog_config.properties.get("length_max_seconds", 43200)
        length_seconds = (
            random.randrange(min_length_seconds, max_length_seconds)
            if max_length_seconds > min_length_seconds
            else min_length_seconds
        )

        return [
            create_catalog_event_for_type(
                catalog_type,
                ts,
                {
                    "uuid": str(uuid.uuid4()),
                    "name": fake.sentence(),
                    "description": fake.sentence(),
                    "duration": length_seconds,
                },
            )
        ]
    elif catalog_type == CatalogType.EXAM:
        min_length_seconds = catalog_config.properties.get("length_min_seconds", 600)
        max_length_seconds = catalog_config.properties.get("length_max_seconds", 7200)

        question_count_min = catalog_config.properties.get("question_count_min", 5)
        question_count_max = catalog_config.properties.get("question_count_max", 30)
        difficulty_min = catalog_config.properties.get("difficulty_min", 0.0)
        difficulty_max = catalog_config.properties.get("difficulty_max", 1.0)

        length_seconds = (
            random.randrange(min_length_seconds, max_length_seconds)
            if max_length_seconds > min_length_seconds
            else min_length_seconds
        )

        exam_uuid = str(uuid.uuid4())
        exam_catalogs = [
            create_catalog_event_for_type(
                CatalogType.EXAM,
                ts,
                {
                    "uuid": exam_uuid,
                    "name": fake.sentence(),
                    "description": fake.sentence(),
                    "duration": length_seconds,
                    "difficulty": get_random_float_in_range(
                        difficulty_min, difficulty_max
                    ),  # 0.0 is not difficult, 1.0 is difficult
                },
            )
        ]

        question_count = get_random_int_in_range(question_count_min, question_count_max)
        for question_index in range(0, question_count):
            exam_catalogs.append(
                create_catalog_event_for_type(
                    CatalogType.QUESTION,
                    ts,
                    {
                        "exam_uuid": exam_uuid,
                        "uuid": str(uuid.uuid4()),
                        "correct_answer_uuid": str(uuid.uuid4()),
                        "wrong_answer_uuids": [str(uuid.uuid4()) for _ in range(0, 4)],
                    },
                )
            )

        return exam_catalogs
    else:
        raise ValueError("Unknown catalog type: %s" % (catalog_type,))


def postprocess_catalog_data(data: Dict[str, Any]) -> Dict[str, Any]:
    if "media_type" in data:
        data["media_type"] = MediaType(data["media_type"])

    return data


def get_catalog_meta_by_uuid(
    db_session: DBSessionWrapper, driver_meta: DriverMetaSchema, catalog_uuid: str
) -> Dict[str, Any]:
    catalog_entry = (
        db_session.query(CatalogEntrySchema).filter_by(driver_meta=driver_meta, platform_uuid=catalog_uuid).one()
    )
    return postprocess_catalog_data(catalog_entry.data)
