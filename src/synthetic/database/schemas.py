import datetime

from sqlalchemy import Integer, Column, DateTime, String, JSON, ForeignKey, UniqueConstraint, Boolean
from sqlalchemy.orm import declarative_base, relationship

from synthetic.constants import MAX_UUID_LENGTH
from synthetic.user.constants import SyntheticUserType

DEFAULT_NAME_LENGTH = 64

Base = declarative_base()


class DriverMetaSchema(Base):
    __tablename__ = 'driver_meta'

    id = Column(Integer, primary_key=True)
    organisation = Column(String(DEFAULT_NAME_LENGTH), nullable=False)
    project = Column(String(DEFAULT_NAME_LENGTH), nullable=False)
    last_seen_ts = Column(DateTime, nullable=True)
    last_maintenance_ts = Column(DateTime, nullable=True)

    driver_data = Column(JSON, nullable=True)

    __table_args__ = (UniqueConstraint('organisation', 'project', name='organisation_project_constraint'),)


class SyntheticUserSchema(Base):
    __tablename__ = 'synthetic_user'

    id = Column(Integer, primary_key=True)
    type = Column(String(DEFAULT_NAME_LENGTH), nullable=False)
    last_seen_ts = Column(DateTime, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)

    platform_uuid = Column(String(MAX_UUID_LENGTH), nullable=False)
    driver_meta_id = Column(Integer, ForeignKey(DriverMetaSchema.id))

    profile_data = Column(JSON, nullable=True)

    driver_meta = relationship('DriverMetaSchema', foreign_keys='SyntheticUserSchema.driver_meta_id')

    @staticmethod
    def create_db_user(
        driver_meta: DriverMetaSchema, last_seen_ts: datetime, user_type: SyntheticUserType
    ) -> "SyntheticUserSchema":
        user = SyntheticUserSchema(last_seen_ts=last_seen_ts, driver_meta=driver_meta, type=user_type.value)
        return user

    @staticmethod
    def create_user_from_data(driver_meta_id: int, user: "SyntheticUser"):
        assert isinstance(driver_meta_id, int)
        db_user = SyntheticUserSchema(
            last_seen_ts=user.last_seen_ts,
            driver_meta_id=driver_meta_id,
            platform_uuid=user.get_platform_uuid(),
            profile_data=user.get_profile_data(),
            is_active=user.is_active(),
            type=user.get_type().value,
        )
        return db_user

    @staticmethod
    def get_user_data(raw_data: "SyntheticUserSchema"):
        return raw_data.user_data

    def update_from_synthetic_user(self, user: "SyntheticUser"):
        self.is_active = user.is_active()
        self.last_seen_ts = user.last_seen_ts
        self.profile_data = user.get_profile_data()


class CatalogEntrySchema(Base):
    __tablename__ = 'catalog_entry'

    id = Column(Integer, primary_key=True)
    type = Column(String(DEFAULT_NAME_LENGTH), nullable=False)
    platform_uuid = Column(String(MAX_UUID_LENGTH), nullable=False)
    data = Column(JSON, nullable=False)

    driver_meta_id = Column(Integer, ForeignKey(DriverMetaSchema.id))

    driver_meta = relationship('DriverMetaSchema', foreign_keys='CatalogEntrySchema.driver_meta_id')
