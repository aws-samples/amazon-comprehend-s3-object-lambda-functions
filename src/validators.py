"""Represents the different validations we would use."""
import json

from constants import REQUEST_ID, REQUEST_TOKEN, REQUEST_ROUTE, GET_OBJECT_CONTEXT, BANNER_CONFIGURATION, INPUT_S3_URL, PAYLOAD
from exceptions import InvalidConfigurationException


class Validator:
    """Generic validator class representing one container which does a particular type of validation."""

    @staticmethod
    def validate(object):
        """Execute the validation."""
        raise NotImplementedError


class JsonValidator(Validator):
    """Simply validates that given string can be converted to Json object or not."""

    @staticmethod
    def validate(json_string: str):
        """Simply validates that given string can be converted to Json object or not."""
        try:
            json.loads(json_string)
        except ValueError:
            raise Exception("Invalid Json %s", json_string)


class InputEventValidator(Validator):
    """Validate the main lambda input."""

    @staticmethod
    def validate(event):
        """Validate the main lambda input."""
        # validations on parts of the event S3 control
        assert BANNER_CONFIGURATION in event
        assert GET_OBJECT_CONTEXT in event
        assert REQUEST_TOKEN in event[GET_OBJECT_CONTEXT]
        assert REQUEST_ROUTE in event[GET_OBJECT_CONTEXT]
        assert REQUEST_ID in event
        assert INPUT_S3_URL in event[GET_OBJECT_CONTEXT]
        assert PAYLOAD in event[BANNER_CONFIGURATION]

        # parts of the event derived from access point configuration
        try:
            JsonValidator.validate(event[BANNER_CONFIGURATION][PAYLOAD])
        except Exception:
            raise InvalidConfigurationException(f"Invalid function payload: {event[BANNER_CONFIGURATION][PAYLOAD]}")
