from enum import Enum


class EventType(Enum):
    PAGE = "page"
    VIDEO = "video"
    AUDIO = "audio"
    IMAGE = "image"
    MODULE = "module"
    SEARCH = "search"
    EXAM = "exam"
    RATE = "rate"


class MediaType(Enum):
    VIDEO = "video"
    AUDIO = "audio"
    IMAGE = "image"


class SubjectType(Enum):
    USER = "user"
    MEDIA = "media"
    SHOP_ITEM = "shop_item"


class NudgeResponseAction(Enum):
    OPEN = "open"
    DISCARD = "discard"
    BLOCK = "block"


class NudgeType(Enum):
    PUSH_NOTIFICATION = "push_notification"


class PaymentType(Enum):
    BANK_TRANSFER = "bank_transfer"
    CHEQUE = "cheque"
    CASH_ON_DELIVERY = "cod"
    CREDIT = "credit"
    POINT_OF_SALE = "pos"
    BANK_CARD = "bank_card"
    OTHER = "other"
