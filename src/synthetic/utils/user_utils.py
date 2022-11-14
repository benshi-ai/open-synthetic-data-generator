import random
import uuid
from typing import Any, Dict, Optional, Tuple
from uuid import uuid4

from faker import Faker

from synthetic.constants import MAX_UUID_LENGTH, PROFILE_NAME_LENGTH_LIMIT

LOCATION_DATA: Dict[str, Any] = {
    "CN": {
        "timezone": "+0800",
        "region_states": {"Huabei": {"cities": ["Beijing", "Tianjin"]}, "Huadong": {"cities": ["Shangai", "Nanjing"]}},
    },
    "IN": {
        "timezone": "+0530",
        "region_states": {"Uttar Pradesh": {"cities": ["Varanasi", "Kanpur"]}, "Rajasthan": {"cities": ["Jaipur"]}},
    },
    "ZA": {"timezone": "+0200", "region_states": {"Western Cape": {"cities": ["Cape Town"]}}},
    "MY": {
        "timezone": "+0800",
        "region_states": {"Central Region": {"cities": ["Kuala Lumpur"]}, "Northern Region": {"cities": ["Penang"]}},
    },
    "MA": {
        "timezone": "+0000",
        "region_states": {"Marrakech-Safi": {"cities": ["Marrakech"]}, "Souss-Massa": {"cities": ["Agadir"]}},
    },
    "ID": {
        "timezone": "+0700",
        "region_states": {"West Java": {"cities": ["Bekasi"]}, "West Kalimantan": {"cities": ["Ketapang"]}},
    },
    "ET": {
        "timezone": "+0300",
        "region_states": {"Somali Region": {"cities": ["Jijiga"]}, "Harari Region": {"cities": ["Harar"]}},
    },
}


def create_user_platform_uuid(profile_name: str):
    if len(profile_name) > PROFILE_NAME_LENGTH_LIMIT:
        profile_name = profile_name[0:PROFILE_NAME_LENGTH_LIMIT]

    platform_uuid = f"{profile_name}-{str(uuid4())}"
    assert len(platform_uuid) <= MAX_UUID_LENGTH

    return platform_uuid


def generate_random_location_data_for_country(country: str) -> Tuple[str, str, int]:
    location_data = LOCATION_DATA[country]
    timezone = location_data["timezone"]
    region_state = random.choice(list(location_data["region_states"].keys()))
    city = random.choice(list(location_data["region_states"][region_state]["cities"]))

    return region_state, city, timezone


def generate_random_user_data(platform_uuid: Optional[str] = None, country: Optional[str] = None) -> Dict[str, str]:
    """We don't want to store this in the db, it's a lot of useless information... So don't actually add it to the
    cache!

    :param platform_uuid:
    :param country:
    :return:
    """
    if country is None:
        country = random.choice(list(LOCATION_DATA.keys()))

    region_state, city, timezone = generate_random_location_data_for_country(country)

    if platform_uuid is None:
        platform_uuid = str(uuid.uuid4())

    return {
        "platform_uuid": platform_uuid,
        "email": fake.email(),
        "name": fake.name(),
        "country": country,
        "timezone": str(timezone),
        "region_state": region_state,
        "city": city,
        "language": random.choice(["de", "en", "es", "fr", "ru", "zh"]),
        "zipcode": str(hash(city)),
        "profession": random.choice(["health worker", "student", "doctor", "nurse"]),
        "workplace": random.choice(["hospital", "primary healthcare center", "secondary healthcare center"]),
        "experience": random.choice(["student", "amateur", "professional"]),
        "organization": fake.name(),
        "education_level": random.choice(
            [
                "primary",
                "lower_secondary",
                "upper_secondary",
                "non_tertiary",
                "tertiary",
                "bachelors",
                "masters",
                "doctorate",
            ]
        ),
    }


fake = Faker()
