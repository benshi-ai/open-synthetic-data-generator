from enum import Enum

SECONDS_IN_DAY = 24 * 60 * 60
VERIFY_SCHEMAS = False


class Currency(Enum):
    USD = "USD"


class CatalogType(Enum):
    APP = "app"
    USER = "user"
    MEDIA_VIDEO = "media_video"
    MEDIA_AUDIO = "media_audio"
    DRUG = "drug"
    BLOOD = "blood"
    OXYGEN = "oxygen"
    ELEARNING_SHOP_ITEM = "elearning_shop_item"
    MEDIA_IMAGE = "media_image"
    MODULE = "module"
    MEDICAL_EQUIPMENT = "medical_equipment"
    EXAM = "exam"
    QUESTION = "question"
    PAGE = "page"
    MILESTONE = "milestone"
    ORDER = "order"
    PROMO = "promo"


class BlockType(Enum):
    CORE = "core"
    ECOMMERCE = "e-commerce"
    ELEARNING = "e-learning"
    PAYMENT = "payment"
    LOYALTY = "loyalty"


class ProductUserType(Enum):
    WEB = "web"
    MOBILE = "mobile"


RAW_UUID_LENGTH = 36
MAX_UUID_LENGTH = 48
PROFILE_NAME_LENGTH_LIMIT = MAX_UUID_LENGTH - RAW_UUID_LENGTH - 1

LOG_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

SUPPORTED_CATALOG_TYPES = [
    CatalogType.USER,
    CatalogType.DRUG,
    CatalogType.BLOOD,
    CatalogType.OXYGEN,
    CatalogType.MEDICAL_EQUIPMENT,
    # CatalogType.MEDIA_IMAGE,
    # CatalogType.MEDIA_VIDEO,
    # CatalogType.MEDIA_AUDIO,
]
