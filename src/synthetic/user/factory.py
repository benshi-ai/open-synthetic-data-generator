import logging
from datetime import datetime
from typing import Optional

from sqlalchemy.orm.attributes import flag_modified

from synthetic.conf import global_conf
from synthetic.database.db_session_wrapper import DBSessionWrapper
from synthetic.database.schemas import SyntheticUserSchema, DriverMetaSchema
from synthetic.user.constants import SyntheticUserType
from synthetic.user.event_per_period_user import EventPerPeriodUser
from synthetic.user.purchase_engagement_user import PurchaseEngagementUser
from synthetic.user.session_engagement_user import SessionEngagementUser
from synthetic.user.synthetic_user import SyntheticUser
from synthetic.utils.database import get_current_memory_usage_kb
from synthetic.utils.random import select_random_keys_from_dict

logger = logging.getLogger(__name__)


def build_user_from_db_data(data: SyntheticUserSchema) -> SyntheticUser:
    user_type = SyntheticUserType(data.type)

    if user_type == SyntheticUserType.SESSION_ENGAGEMENT:
        return SessionEngagementUser.from_db_data(data)
    elif user_type == SyntheticUserType.PURCHASE_ENGAGEMENT:
        return PurchaseEngagementUser.from_db_data(data)
    elif user_type == SyntheticUserType.EVENT_PER_PERIOD:
        return EventPerPeriodUser.from_db_data(data)
    else:
        raise ValueError("Invalid synthetic user type: %s" % (data.type,))


def load_users_from_db(db_session: DBSessionWrapper, driver_meta_id: int, active_only: bool = False):
    assert isinstance(driver_meta_id, int)
    user_data_list = db_session.query(SyntheticUserSchema).filter_by(driver_meta_id=driver_meta_id)

    if active_only:
        user_data_list = user_data_list.filter_by(is_active=True)

    user_data_list = user_data_list.order_by(SyntheticUserSchema.platform_uuid).all()
    users = [build_user_from_db_data(user) for user in user_data_list]
    return users


def load_user_from_db(db_session: DBSessionWrapper, driver_meta_id: int, platform_uuid: str) -> "SyntheticUser":
    logger.debug("Memory - before loading user: %s", get_current_memory_usage_kb())

    assert isinstance(driver_meta_id, int)
    loaded_db_user = (
        db_session.query(SyntheticUserSchema)
        .filter_by(driver_meta_id=driver_meta_id, platform_uuid=platform_uuid)
        .one()
    )

    logger.debug("Memory - after loading user: %s", get_current_memory_usage_kb())
    user = build_user_from_db_data(loaded_db_user)
    logger.debug("Memory - after building user: %s", get_current_memory_usage_kb())
    return user


def clear_users_from_db(db_session: DBSessionWrapper, driver_meta: DriverMetaSchema):
    db_session.query(SyntheticUserSchema).filter_by(driver_meta=driver_meta).delete()
    db_session.commit()


def store_user_in_db(db_session: DBSessionWrapper, driver_meta_id: int, user: SyntheticUser):
    assert isinstance(driver_meta_id, int)
    db_user = (
        db_session.query(SyntheticUserSchema)
        .filter_by(driver_meta_id=driver_meta_id, platform_uuid=user.get_platform_uuid())
        .first()
    )
    if db_user is None:
        db_user = SyntheticUserSchema.create_user_from_data(driver_meta_id, user)
        db_session.add(db_user)
    else:
        db_user.update_from_synthetic_user(user)

    # SQLAlchemy doesn't automatically detect changes to this field, so let's just always overwrite
    flag_modified(db_user, "profile_data")


def create_random_user(
    driver_meta_id: int,
    registration_ts: datetime,
    profile_name: Optional[str] = None,
    platform_uuid: Optional[str] = None,
):
    assert isinstance(driver_meta_id, int)

    if profile_name is None:
        profile_name = select_random_keys_from_dict(global_conf.profiles)[0]

    profile_config = global_conf.profiles[profile_name]
    user_type = profile_config.user_type

    if user_type == SyntheticUserType.SESSION_ENGAGEMENT:
        return SessionEngagementUser.create_random_user(
            driver_meta_id=driver_meta_id,
            registration_ts=registration_ts,
            profile_name=profile_name,
            platform_uuid=platform_uuid,
        )
    elif user_type == SyntheticUserType.PURCHASE_ENGAGEMENT:
        return PurchaseEngagementUser.create_random_user(
            driver_meta_id=driver_meta_id,
            registration_ts=registration_ts,
            profile_name=profile_name,
            platform_uuid=platform_uuid,
        )
    elif user_type == SyntheticUserType.EVENT_PER_PERIOD:
        return EventPerPeriodUser.create_random_user(
            driver_meta_id=driver_meta_id,
            registration_ts=registration_ts,
            profile_name=profile_name,
            platform_uuid=platform_uuid,
        )
    else:
        raise ValueError("Unsupported profile type: %s" % (user_type,))
