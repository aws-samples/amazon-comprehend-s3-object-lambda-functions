from unittest import TestCase

from data_object import PiiConfig
from exceptions import InvalidConfigurationException


class DataObjectTest(TestCase):

    def test_Pii_config_valid_confidence_threshold(self):
        with self.assertRaises(InvalidConfigurationException) as e:
            PiiConfig(confidence_threshold=0.1)
        assert e.exception.message == 'CONFIDENCE_THRESHOLD is not within allowed range [0.5,1]'