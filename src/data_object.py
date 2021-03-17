"""Module containing some custom data structures ."""

import os
from typing import List

from exceptions import InvalidConfigurationException


class PiiConfig:
    """PiiConfig class represents the base config for classification and redaction."""

    def __init__(self, pii_entity_types: List = None,
                 confidence_threshold: float = os.getenv('CONFIDENCE_THRESHOLD', 0.5),
                 **kwargs):
        self.pii_entity_types = pii_entity_types
        self.confidence_threshold = float(confidence_threshold)
        if not 0.5 <= self.confidence_threshold <= 1.0:
            raise InvalidConfigurationException('CONFIDENCE_THRESHOLD is not within allowed range [0.5,1]')
        if self.pii_entity_types is None:
            self.pii_entity_types = os.getenv('PII_ENTITY_TYPES', 'ALL').split(',')


class ClassificationConfig(PiiConfig):
    """ClassificationConfig class represents the config to be used for classification."""

    def __init__(self, pii_entity_types: List = None,
                 confidence_threshold: float = os.getenv('CONFIDENCE_THRESHOLD', 0.5),
                 **kwargs):
        super().__init__(pii_entity_types, confidence_threshold, **kwargs)


class RedactionConfig(ClassificationConfig):
    """RedactionConfig class represents the config to be used for redaction."""

    def __init__(self, pii_entity_types: List = None, mask_mode: str = os.getenv('MASK_MODE', 'MASK'),
                 mask_character: str = os.getenv('MASK_CHARACTER', '*'),
                 confidence_threshold: float = os.getenv('CONFIDENCE_THRESHOLD', 0.5),
                 **kwargs):
        super().__init__(pii_entity_types, confidence_threshold, **kwargs)
        self.mask_character = mask_character
        self.mask_mode = mask_mode


class Document:
    """A chunk of text."""

    def __init__(self, text: str, char_offset: int = 0, pii_classification: map = {},
                 pii_entities: List = [], redacted_text: str = ''):
        self.text = text
        self.char_offset = char_offset
        self.pii_classification = pii_classification
        self.pii_entities = pii_entities
        self.redacted_text = redacted_text
