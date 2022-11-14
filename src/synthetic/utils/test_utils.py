import re
import logging
import os
from re import Pattern

from typing import Any, Dict

import yaml
from mock.mock import MagicMock, call

from synthetic import DATA_DIRNAME
from synthetic.constants import SUPPORTED_CATALOG_TYPES, VERIFY_SCHEMAS
from synthetic.event.catalog.catalog_base import CatalogEvent
from synthetic.event.event_collection import EventCollection
from synthetic.event.log.log_base import LogEvent

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)

CURRENCY_PATTERN = re.compile("^[A-Z]{3}$")
TZ_PATTERN = re.compile("^[+-]\\d{4}$")
COUNTRY_PATTERN = re.compile("^[A-Z]{2}$")
LANGUAGE_PATTERN = re.compile("^[a-z]{2}$")


def assert_dicts_equal_partial(found, expected):
    for key, value in expected.items():
        assert value == found[key], f"Mismatch on {key}: expected {value} != found '{found[key]}'"


def call_args_match(expected_arg: Any, found_arg: Any) -> bool:
    return bool(expected_arg == found_arg)


def assert_mock_function_has_call(mock_function: MagicMock, expected_call: call):
    for found_call in mock_function.call_args_list:
        if len(found_call.args) != len(expected_call.args):
            continue

        arg_mismatch = False
        for expected_arg, found_arg in zip(found_call.args, expected_call.args):
            if not call_args_match(expected_arg, found_arg):
                arg_mismatch = True
                break

        if arg_mismatch:
            continue

        if len(found_call.kwargs) != len(expected_call.kwargs):
            continue

        kwarg_mismatch = False
        for key in found_call.kwargs:
            found_arg = found_call.kwargs[key]
            expected_arg = expected_call.kwargs[key]

            if not call_args_match(expected_arg, found_arg):
                kwarg_mismatch = True
                break

        if kwarg_mismatch:
            continue

        # Match!
        return

    raise AssertionError("Expected call not found!")


class SchemaLoader:
    LOADED_SCHEMAS: Dict[str, Dict] = {}

    @classmethod
    def _load_required_schema(cls, schema_name: str) -> Dict:
        with open(os.path.join(DATA_DIRNAME, f"{schema_name}.yml"), "r") as yaml_file:
            config_data = yaml.safe_load(yaml_file)

        return config_data

    @classmethod
    def get_schema(cls, schema_name: str) -> Dict:
        if schema_name not in cls.LOADED_SCHEMAS:
            cls.LOADED_SCHEMAS[schema_name] = cls._load_required_schema(schema_name)

        return cls.LOADED_SCHEMAS[schema_name]


def prop_has_type(value: Any, type_str: str) -> bool:
    if type_str == "string":
        return isinstance(value, str)
    elif type_str == "float":
        return isinstance(value, float)
    elif type_str == "boolean":
        return isinstance(value, bool)
    elif type_str == "int":
        return isinstance(value, int)
    elif type_str == "object":
        return True
    elif type_str == "string[]":
        assert isinstance(value, list)
        for sub_value in value:
            assert isinstance(sub_value, str)
        return True
    else:
        object_type_parts = type_str.split("[")
        if len(object_type_parts) == 1:
            try:
                assert_props_have_schema(
                    value, SchemaLoader.get_schema("ingest_schema")["data_types"][object_type_parts[0]]["properties"]
                )
                return True
            except AssertionError:
                return False
        elif len(object_type_parts) == 2:
            try:
                assert isinstance(value, list)
                for sub_value in value:
                    assert_props_have_schema(
                        sub_value,
                        SchemaLoader.get_schema("ingest_schema")["data_types"][object_type_parts[0]]["properties"],
                    )
                return True
            except AssertionError:
                return False
        else:
            raise ValueError("Invalid type str: %s" % (type_str,))


def assert_match_pattern_format(value: str, pattern: Pattern):
    assert bool(pattern.match(value)), "%s doesn't match %s" % (value, pattern)


def assert_prop_has_schema(prop_name: str, prop_value: Any, schema: Dict):
    expected_prop_schema = schema[prop_name]

    logger.debug("Checking %s = %s has schema %s...", prop_name, prop_value, expected_prop_schema)

    if "type" not in expected_prop_schema:
        raise ValueError(expected_prop_schema)
    assert prop_has_type(prop_value, expected_prop_schema["type"]), "%s is %s that doesn't have type %s" % (
        prop_name,
        prop_value,
        expected_prop_schema["type"],
    )

    if prop_name.lower() == "currency":
        assert_match_pattern_format(prop_value, CURRENCY_PATTERN)

    if "enum" in expected_prop_schema:
        pattern_format = None
        if prop_name == "timezone":
            pattern_format = TZ_PATTERN
        elif prop_name == "country":
            pattern_format = COUNTRY_PATTERN
        elif prop_name == "language":
            pattern_format = LANGUAGE_PATTERN

        if pattern_format is not None:
            assert_match_pattern_format(expected_prop_schema["example"], pattern_format)
            assert_match_pattern_format(prop_value, pattern_format)
        else:
            assert prop_value in expected_prop_schema["enum"]


def assert_props_have_schema(event_props, schema):
    for prop_name, prop_value in event_props.items():
        assert_prop_has_schema(prop_name, prop_value, schema)


def log_event_has_schema(event: LogEvent, full_schema: Dict) -> bool:
    schema = full_schema
    ingest_path = event.get_schema_path()
    if ingest_path is None:
        return True

    for section in ingest_path.split("/"):
        schema = schema[section]

    logger.debug("Confirming %s matches log schema", event.__class__.__name__)
    assert event.block.value == schema["block"]
    for prop_name in schema["props"]["required"]:
        logger.debug("Checking for required prop: %s", prop_name)
        require_property = True
        if "(" in prop_name:
            assert ")" in prop_name
            require_property = False
            # This is a conditional prop
            condition = prop_name[prop_name.index("(") : prop_name.index(")") + 1][1:-1]
            required_property = condition.split("=")[0].strip()
            required_value = condition.split("=")[1].strip()
            if str(event.props[required_property]) == required_value:
                require_property = True
            prop_name = prop_name.split(" ")[0]

        if require_property:
            assert prop_name in event.props, "%s not found in %s!" % (prop_name, event.props.keys())

    logger.debug("Checking for matching schema: %s", schema["props"]["properties"])
    assert_props_have_schema(event.props, schema["props"]["properties"])

    return True


def catalog_event_has_schema(event: CatalogEvent, full_schema: Dict) -> bool:
    schema = full_schema
    catalog_path = event.get_schema_path()
    logger.debug("Getting headers for %s...", event)

    if catalog_path is not None:
        for section in catalog_path.split("/"):
            schema = schema[section]

        catalog_backend_data = event.get_backend_data()
        logger.critical("Confirming %s matches catalog schema", event.__class__.__name__)
        reference_schema = schema['headers']
        for header_name in reference_schema:
            assert header_name in catalog_backend_data, "%s not found in %s!" % (
                header_name,
                catalog_backend_data.keys(),
            )

        logger.debug("Checking for matching schema: %s", sorted(list(schema.keys())))
        assert_props_have_schema(catalog_backend_data, reference_schema)

    return True


def assert_events_have_correct_schema(events: EventCollection):
    if not VERIFY_SCHEMAS:
        return

    ingest_schema = SchemaLoader.get_schema("ingest_schema")
    for log_event in events.log_events:
        assert log_event_has_schema(log_event, ingest_schema), f"{log_event} doesn't match schema!"

        logger.debug("Checking for log payload match...")
        payload_schema = ingest_schema["main_ingest_body"]
        for prop_name, prop_value in log_event.as_payload_dict().items():
            try:
                assert_prop_has_schema(prop_name, prop_value, payload_schema)
            except AssertionError:
                raise
    catalog_schema = SchemaLoader.get_schema("catalog_schema")
    for catalog_event in events.catalog_events:
        if catalog_event.catalog_type not in SUPPORTED_CATALOG_TYPES:
            logger.debug("Not verifying: %s" % (catalog_event.catalog_type,))
            continue

        assert catalog_event_has_schema(catalog_event, catalog_schema), f"{catalog_event} doesn't match schema!"

        logger.debug("Checking for catalog payload match...")
        payload_schema = catalog_schema
        catalog_payload = catalog_event.as_csv_dict()

        assert catalog_payload["subject_type"] in payload_schema
