import json
import os
from copy import deepcopy
from unittest import TestCase

from constants import BANNER_CONFIGURATION, GET_OBJECT_CONTEXT, REQUEST_TOKEN, REQUEST_ROUTE, REQUEST_ID, INPUT_S3_URL, PAYLOAD
from validators import JsonValidator, Validator, InputEventValidator

this_module_path = os.path.dirname(__file__)


class TestValidator(TestCase):
    def test_json_validator(self):
        with self.assertRaises(Exception) as context:
            JsonValidator.validate("{\"invalid\":json_string")

    def test_validator_interface(self):
        with self.assertRaises(NotImplementedError) as context:
            Validator.validate(None)

    def test_input_event_validation(self):
        with open(os.path.join(this_module_path, "..", 'data', 'sample_event.json'), 'r') as file_pointer:
            sample_event = json.load(file_pointer)

        with self.assertRaises(AssertionError) as context:
            InputEventValidator.validate({})
        with self.assertRaises(AssertionError) as context:
            invalid_event = deepcopy(sample_event)
            del invalid_event[BANNER_CONFIGURATION]
            InputEventValidator.validate(invalid_event)
        with self.assertRaises(AssertionError) as context:
            invalid_event = deepcopy(sample_event)
            del invalid_event[GET_OBJECT_CONTEXT]
            InputEventValidator.validate(invalid_event)
        with self.assertRaises(AssertionError) as context:
            invalid_event = deepcopy(sample_event)
            del invalid_event[GET_OBJECT_CONTEXT][REQUEST_TOKEN]
            InputEventValidator.validate(invalid_event)
        with self.assertRaises(AssertionError) as context:
            invalid_event = deepcopy(sample_event)
            del invalid_event[GET_OBJECT_CONTEXT][REQUEST_ROUTE]
            InputEventValidator.validate(invalid_event)
        with self.assertRaises(AssertionError) as context:
            invalid_event = deepcopy(sample_event)
            del invalid_event[REQUEST_ID]
            InputEventValidator.validate(invalid_event)
        with self.assertRaises(AssertionError) as context:
            invalid_event = deepcopy(sample_event)
            del invalid_event[GET_OBJECT_CONTEXT][INPUT_S3_URL]
            InputEventValidator.validate(invalid_event)
        with self.assertRaises(AssertionError) as context:
            invalid_event = deepcopy(sample_event)
            del invalid_event[BANNER_CONFIGURATION][PAYLOAD]
            InputEventValidator.validate(invalid_event)
        with self.assertRaises(Exception) as context:
            invalid_event = deepcopy(sample_event)
            invalid_event[BANNER_CONFIGURATION][PAYLOAD] = "Invalid json"
            InputEventValidator.validate(invalid_event)
