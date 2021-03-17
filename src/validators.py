"""Represents the different validations we would use."""
import json

from config import IS_PARTIAL_OBJECT_SUPPORTED
from constants import REQUEST_ID, REQUEST_TOKEN, REQUEST_ROUTE, GET_OBJECT_CONTEXT, S3OL_CONFIGURATION, INPUT_S3_URL, PAYLOAD, \
    PART_NUMBER, RANGE, USER_REQUEST, HEADERS
from exceptions import InvalidConfigurationException, InvalidRequestException


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


class PartialObjectRequestValidator(Validator):
    """Validates that the GetObject request is not for a partial object."""

    @staticmethod
    def validate(input_event: str):
        """Perform the validation."""
        RESTRICTED_HEADERS = [RANGE, PART_NUMBER]
        if not IS_PARTIAL_OBJECT_SUPPORTED:
            if HEADERS in input_event[USER_REQUEST]:
                for header in input_event[USER_REQUEST][HEADERS]:
                    if header in RESTRICTED_HEADERS:
                        raise InvalidRequestException(f"HTTP Header {header} is not supported")


class InputEventValidator(Validator):
    """Validate the main lambda input."""

    @staticmethod
    def validate(event):
        """Validate the main lambda input."""
        # validations on parts of the event S3 control
        assert S3OL_CONFIGURATION in event
        assert GET_OBJECT_CONTEXT in event
        assert REQUEST_TOKEN in event[GET_OBJECT_CONTEXT]
        assert REQUEST_ROUTE in event[GET_OBJECT_CONTEXT]
        assert REQUEST_ID in event
        assert INPUT_S3_URL in event[GET_OBJECT_CONTEXT]
        assert PAYLOAD in event[S3OL_CONFIGURATION]

        # parts of the event derived from access point configuration
        try:
            if event[S3OL_CONFIGURATION][PAYLOAD]:
                JsonValidator.validate(event[S3OL_CONFIGURATION][PAYLOAD])
        except Exception:
            raise InvalidConfigurationException(f"Invalid function payload: {event[S3OL_CONFIGURATION][PAYLOAD]}")
