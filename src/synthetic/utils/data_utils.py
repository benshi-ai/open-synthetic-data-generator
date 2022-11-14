from enum import Enum


def prepare_data_for_db(data):
    for key, value in data.copy().items():
        if isinstance(value, Enum):
            data[key] = value.value
        elif isinstance(value, dict):
            data[key] = prepare_data_for_db(value)

    return data
