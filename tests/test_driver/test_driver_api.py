import json
from unittest.mock import Mock
from uuid import uuid4

import pytest
import random

from datetime import datetime

from requests import Response

from synthetic.constants import CatalogType
from synthetic.conf import ProfileConfig, global_conf
from synthetic.driver.driver import Driver
from synthetic.event.catalog.drug_catalog import DrugCatalogEvent
from synthetic.event.catalog.media_catalog import MediaCatalogEvent
from synthetic.event.catalog.user_catalog import UserCatalogEvent
from synthetic.event.event_collection import EventCollection
from synthetic.event.log.general.page import PageEvent
from synthetic.user.session_engagement_user import SessionEngagementUser
from synthetic.utils.test_utils import assert_events_have_correct_schema


@pytest.fixture(autouse=True)
def fixed_seed():
    random.seed(0)


@pytest.fixture(autouse=True)
def configure_profiles():
    global_conf.api_url = "http://www.test.com"
    global_conf.api_key = "test key"

    global_conf.profiles = {"boring_guy": ProfileConfig()}


@pytest.fixture
def last_seen_ts():
    return datetime(2000, 1, 1, 0, 0, 0)


@pytest.fixture
def synthetic_user(driver_meta, last_seen_ts):
    user = SessionEngagementUser(
        driver_meta_id=driver_meta.id,
        platform_uuid=str(uuid4()),
        profile_data=SessionEngagementUser.create_initial_profile_data("boring_guy", registration_ts=last_seen_ts),
        last_seen_ts=last_seen_ts,
    )
    return user


def test_send_page_event(mocker, synthetic_user, last_seen_ts):
    response = Response()
    response.status_code = 200

    mock_post = mocker.patch('requests.post', return_value=Mock(status_code=200))

    event = PageEvent(
        synthetic_user, last_seen_ts, online=True, uuid="bla", path="/app/check", title="Checking the app", duration=66
    )
    driver = Driver(sink_types=["http"])
    driver.queue_events_for_flush(EventCollection(log_events=[event]))
    driver.flush_events(last_seen_ts)

    mock_post.assert_called_with(
        url="%s/data/ingest/log" % (global_conf.api_url,),
        data=json.dumps({"data": [event.as_payload_dict()]}),
        headers={'Authorization': 'Bearer test key', 'Content-Type': 'application/json'},
    )


def test_send_user_catalog_event(mocker, synthetic_user, last_seen_ts):
    response = Response()
    response.status_code = 200

    mock_post = mocker.patch('requests.post', return_value=Mock(status_code=200))

    event_data = {
        "name": "johnny",
        "platform_uuid": "plat_id",
        "country": "XZ",
        "experience": "none",
        "region_state": "region_state",
        "education_level": "primary",
        "organization": "acme",
        "city": "city",
        "language": "xy",
        "zipcode": "08036",
        "workplace": "workplace",
        "profession": "profession",
        "timezone": "+1234",
    }
    event = UserCatalogEvent(
        last_seen_ts,
        data=event_data,
    )
    driver = Driver(sink_types=["http"])
    driver.queue_events_for_flush(EventCollection(catalog_events=[event]))
    driver.flush_events(last_seen_ts)

    assert_events_have_correct_schema(EventCollection(catalog_events=[event]))

    expected_call_data = [
        {
            'id': 'plat_id',
            'name': 'johnny',
            'country': event_data['country'],
            'region_state': 'region_state',
            'city': 'city',
            'workplace': 'workplace',
            'timezone': event_data['timezone'],
            'profession': 'profession',
            'zipcode': '08036',
            'language': 'xy',
            'experience': 'none',
            'education_level': event_data['education_level'],
            'organization': 'acme',
        },
    ]

    mock_post.assert_called_with(
        url="%s/data/ingest/catalog/user" % (global_conf.api_url,),
        data=json.dumps(expected_call_data),
        headers={'Authorization': 'Bearer test key', 'Content-Type': 'application/json'},
    )


@pytest.mark.skip("Disabled until backend supports media catalogs on new endpoint")
def test_send_video_item_catalog_event(mocker, synthetic_user, last_seen_ts):
    response = Response()
    response.status_code = 200

    mock_post = mocker.patch('requests.post', return_value=Mock(status_code=200))

    event_data = {
        "uuid": "my_uuid",
        "media_type": "video",
        "experience": "none",
        "name": "video_name",
        "description": "video_description",
        "lang": "es",
        "length": 123.0,
        "resolution": "360",
    }
    event = MediaCatalogEvent(
        CatalogType.MEDIA_VIDEO,
        last_seen_ts,
        data=event_data,
    )
    driver = Driver(sink_types=["http"])
    driver.queue_events_for_flush(EventCollection(catalog_events=[event]))
    driver.flush_events(last_seen_ts)

    assert_events_have_correct_schema(EventCollection(catalog_events=[event]))

    expected_call_data = [
        {
            'id': 'video_my_uuid',
            'id_source': 'my_uuid',
            'name': 'video_name',
            'type': 'video',
            'length': 123.0,
            'description': 'video_description',
            'resolution': '360',
            'language': event_data['lang'],
        }
    ]

    mock_post.assert_called_with(
        url="%s/data/ingest/catalog/media" % (global_conf.api_url,),
        data=json.dumps(expected_call_data),
        headers={'Authorization': 'Bearer test key', 'Content-Type': 'application/json'},
    )


def test_send_drug_catalog_event(mocker, synthetic_user, last_seen_ts):
    response = Response()
    response.status_code = 200

    mock_post = mocker.patch('requests.post', return_value=Mock(status_code=200))

    event = DrugCatalogEvent(
        CatalogType.DRUG,
        last_seen_ts,
        data={
            "uuid": "a_drug_id",
            "drug_name": "a_drug_name",
            "market_id": "a_market_id",
            "description": "a_description",
            "supplier_id": "a_supplier_id",
            "supplier_name": "a_supplier_name",
            "active_ingredients": "a_active_ingredients",
            "producer": "a_producer",
            "packaging": "a_packaging",
            "drug_form": "a_drug_form",
            "drug_strength": "a_drug_strength",
            "atc_anatomical_group": "a_atc_anatomical_group",
            "otc_or_ethical": "a_otc_or_ethical",
        },
    )

    driver = Driver(sink_types=["http"])
    driver.queue_events_for_flush(EventCollection(catalog_events=[event]))
    driver.flush_events(last_seen_ts)

    assert_events_have_correct_schema(EventCollection(catalog_events=[event]))

    expected_call_data = [
        {
            "id": "a_drug_id",
            "name": "a_drug_name",
            "market_id": "a_market_id",
            "description": "a_description",
            "supplier_id": "a_supplier_id",
            "supplier_name": "a_supplier_name",
            "active_ingredients": "a_active_ingredients",
            "producer": "a_producer",
            "packaging": "a_packaging",
            "drug_name": "a_drug_name",
            "drug_form": "a_drug_form",
            "drug_strength": "a_drug_strength",
            "atc_anatomical_group": "a_atc_anatomical_group",
            "otc_or_ethical": "a_otc_or_ethical",
        }
    ]

    mock_post.assert_called_with(
        url="%s/data/ingest/catalog/drug" % (global_conf.api_url,),
        data=json.dumps(expected_call_data),
        headers={'Authorization': 'Bearer test key', 'Content-Type': 'application/json'},
    )
