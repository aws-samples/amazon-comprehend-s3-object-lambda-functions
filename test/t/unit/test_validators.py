import json
import os
from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from config import IS_PARTIAL_OBJECT_SUPPORTED
from constants import S3OL_CONFIGURATION, GET_OBJECT_CONTEXT, REQUEST_TOKEN, REQUEST_ROUTE, REQUEST_ID, INPUT_S3_URL, PAYLOAD, \
    USER_REQUEST, HEADERS, RANGE
from exceptions import InvalidConfigurationException, InvalidRequestException
from validators import JsonValidator, Validator, InputEventValidator, PartialObjectRequestValidator

this_module_path = os.path.dirname(__file__)


class TestValidator(TestCase):
    def setUp(self) -> None:
        with open(os.path.join(this_module_path, "..", 'data', 'sample_event.json'), 'r') as file_pointer:
            self.sample_event = json.load(file_pointer)
        self.is_partial_object_supported = IS_PARTIAL_OBJECT_SUPPORTED

    def test_json_validator(self):
        with self.assertRaises(Exception) as context:
            JsonValidator.validate("{\"invalid\":json_string")

    def test_validator_interface(self):
        with self.assertRaises(NotImplementedError) as context:
            Validator.validate(None)

    def test_partial_object_validator_partial_object_requested(self):
        with self.assertRaises(InvalidRequestException) as context:
            self.sample_event[USER_REQUEST][HEADERS][RANGE] = "0-100"
            PartialObjectRequestValidator.validate(self.sample_event)
        assert context.exception.message == "HTTP Header Range is not supported"


    def test_partial_object_validator_complete_object_requested(self):
        temp = deepcopy(self.sample_event)
        PartialObjectRequestValidator.validate(temp)

    @patch('validators.IS_PARTIAL_OBJECT_SUPPORTED', True)
    def test_partial_object_validator_when_partial_object_is_supported(self):
        self.sample_event[USER_REQUEST][HEADERS][RANGE] = "0-100"
        PartialObjectRequestValidator.validate(self.sample_event)

    def test_input_event_validation_empty_input(self):
        with self.assertRaises(AssertionError) as context:
            InputEventValidator.validate({})

    def test_input_event_validation_missing_s3ol_config(self):
        with self.assertRaises(AssertionError) as context:
            invalid_event = deepcopy(self.sample_event)
            del invalid_event[S3OL_CONFIGURATION]
            InputEventValidator.validate(invalid_event)

    def test_input_event_validation_missing_object_context(self):
        with self.assertRaises(AssertionError) as context:
            invalid_event = deepcopy(self.sample_event)
            del invalid_event[GET_OBJECT_CONTEXT]
            InputEventValidator.validate(invalid_event)

    def test_input_event_validation_missing_request_token(self):
        with self.assertRaises(AssertionError) as context:
            invalid_event = deepcopy(self.sample_event)
            del invalid_event[GET_OBJECT_CONTEXT][REQUEST_TOKEN]
            InputEventValidator.validate(invalid_event)

    def test_input_event_validation_missing_request_route(self):
        with self.assertRaises(AssertionError) as context:
            invalid_event = deepcopy(self.sample_event)
            del invalid_event[GET_OBJECT_CONTEXT][REQUEST_ROUTE]
            InputEventValidator.validate(invalid_event)

    def test_input_event_validation_missing_request_id(self):
        with self.assertRaises(AssertionError) as context:
            invalid_event = deepcopy(self.sample_event)
            del invalid_event[REQUEST_ID]
            InputEventValidator.validate(invalid_event)

    def test_input_event_validation_missing_input_s3_url(self):
        with self.assertRaises(AssertionError) as context:
            invalid_event = deepcopy(self.sample_event)
            del invalid_event[GET_OBJECT_CONTEXT][INPUT_S3_URL]
            InputEventValidator.validate(invalid_event)

    def test_input_event_validation_missing_payload(self):
        with self.assertRaises(AssertionError) as context:
            invalid_event = deepcopy(self.sample_event)
            del invalid_event[S3OL_CONFIGURATION][PAYLOAD]
            InputEventValidator.validate(invalid_event)

    def test_input_event_validation_invalid_payload(self):
        with self.assertRaises(InvalidConfigurationException) as context:
            invalid_event = deepcopy(self.sample_event)
            invalid_event[S3OL_CONFIGURATION][PAYLOAD] = "Invalid json"
            InputEventValidator.validate(invalid_event)

    def test_input_event_validation_empty_payload(self):
        invalid_event = deepcopy(self.sample_event)
        invalid_event[S3OL_CONFIGURATION][PAYLOAD] = ""
        InputEventValidator.validate(invalid_event)
