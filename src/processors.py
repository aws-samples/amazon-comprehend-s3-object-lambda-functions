"""Text processors."""

# must be the first import in files with lambda function handlers
from copy import deepcopy
from typing import List

import lambdalogging
from config import SUBSEGMENT_OVERLAPPING_TOKENS, MAX_CHARS_OVERLAP
from constants import ENTITY_TYPE, BEGIN_OFFSET, END_OFFSET, ALL, REPLACE_WITH_PII_ENTITY_TYPE, SCORE
from data_object import Document
from data_object import RedactionConfig
from exceptions import InvalidConfigurationException

LOG = lambdalogging.getLogger(__name__)


class Segmenter:
    """Offer functionality to segment and desegment."""

    def __init__(self, max_doc_size: int, overlap_tokens: int = SUBSEGMENT_OVERLAPPING_TOKENS,
                 max_overlapping_chars: int = MAX_CHARS_OVERLAP, **kwargs):
        self.max_overlapping_chars = int(max_overlapping_chars)
        self.overlap_tokens = int(overlap_tokens)
        self.max_doc_size = int(max_doc_size)
        # A utf8 character can go upto 4 bytes
        if max_doc_size < 4:
            raise InvalidConfigurationException(
                f"Maximum text size limit ({self.max_doc_size} bytes) is too less to perform segmentation")

    def _trim_to_max_bytes(self, s, max_bytes):
        """
        Ensure that the UTF-8 encoding of a string has not more than max_bytes bytes.

        The table below summarizes the format of these different octet types.
           Char. number range  |        UTF-8 octet sequence
              (hexadecimal)    |              (binary)
           --------------------+---------------------------------------------
           0000 0000-0000 007F | 0xxxxxxx
           0000 0080-0000 07FF | 110xxxxx 10xxxxxx
           0000 0800-0000 FFFF | 1110xxxx 10xxxxxx 10xxxxxx
           0001 0000-0010 FFFF | 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
        """

        def safe_b_of_i(b, i):
            try:
                return b[i]
            except IndexError:
                return 0

        # Edge cases
        if s == '' or max_bytes < 1:
            return ''

        # cut it twice to avoid encoding potentially GBs of string just to get e.g. 10 bytes?
        bytes_array = s[:max_bytes].encode('utf-8')[:max_bytes]

        # find the first byte from end which contains the starting byte of a utf8 character which is this format 11xxxxxx for
        # multi byte character. For single byte character the format is 0xxxxxxx as described above
        if bytes_array[-1] & 0b10000000:
            last_11xxxxxx_index = [
                i
                for i in range(-1, -5, -1)
                if safe_b_of_i(bytes_array, i) & 0b11000000 == 0b11000000
            ][0]
            # As described above in the table , we can determine the total size(in bytes) of char from the first byte itself
            starting_byte = bytes_array[last_11xxxxxx_index]
            if not starting_byte & 0b00100000:
                last_char_length = 2
            elif not starting_byte & 0b00010000:
                last_char_length = 3
            elif not starting_byte & 0b00001000:
                last_char_length = 4
            else:
                raise Exception(f"Unexpected utf-8 {starting_byte} byte encountered")

            if last_char_length > -last_11xxxxxx_index:
                # remove the incomplete character
                bytes_array = bytes_array[:last_11xxxxxx_index]

        return bytes_array.decode('utf-8')

    def _trim_partial_trailing_word(self, text):
        # find the first space moving backwards
        original_length = len(text)
        k = original_length - 1
        # ensuring we have a hard limit on how back we need to travel. We don't want to travel the whole sentence back
        # if there are no spaces in it. Using max_overlapping_chars as proxy for this
        while text[k] != ' ' and k > 0 and original_length - k < self.max_overlapping_chars:
            k -= 1
        trimmed_text = text[:k + 1]
        return trimmed_text

    def _find_trailing_overlapping_tokens_start_index(self, text):
        word_count = 0
        original_length = len(text)
        k = original_length - 1
        while word_count < self.overlap_tokens:
            k -= 1
            # Moving backwards: find the beginning of word (next character is space and current character is not space)
            while not (text[k + 1] != ' ' and text[k] == ' ') and k > 0 and original_length - k < self.max_overlapping_chars:
                k -= 1
            word_count += 1
            if k == 0:
                LOG.debug("Overlapping tokens for the next sentence starts beyond the current sentence")
                break
        return k

    def _merge_classifcation_results(self, segment: Document, existing_results: map = {}):
        for name, score in segment.pii_classification.items():
            if name not in existing_results or (
                    name in existing_results and score > existing_results[name]):
                existing_results[name] = score
        return existing_results

    def _is_overlapping_annotations(self, entity_a, entity_b) -> int:
        """
        Determine if one entity overlaps with another.
        It will return :
         1 if entity_b, lies on right side of the entity
         0 if entity_b overlaps with entity_a
         -1 if entity_b lies on left side of the entity
        """
        if entity_a[END_OFFSET] < entity_b[BEGIN_OFFSET]:
            return 1
        if entity_a[BEGIN_OFFSET] > entity_b[END_OFFSET]:
            return -1
        else:
            return 0

    def _resolve_overlapped_annotation(self, entity_a, entity_b) -> List:
        """Merge two overlapping entity annotations."""
        if entity_a[SCORE] >= entity_b[SCORE]:
            return [entity_a]
        else:
            return [entity_b]

    def _merge_pii_annotation_results(self, segment: Document, existing_annotations: List = []):

        if not existing_annotations:
            existing_annotations.extend(segment.pii_entities)
            return

        for pii_entity in segment.pii_entities:
            k = len(existing_annotations) - 1
            while k > 0:
                overlap_result = self._is_overlapping_annotations(existing_annotations[k], pii_entity)
                if overlap_result > 0:
                    existing_annotations.append(pii_entity)
                    break
                elif overlap_result == 0:
                    LOG.debug("Annotation: " + str(existing_annotations[k]) + " conflicts with: " + str(pii_entity))
                    resolved_annotation = self._resolve_overlapped_annotation(existing_annotations[k], pii_entity)
                    LOG.debug("Deleting annotation:" + str(existing_annotations[k]))
                    del existing_annotations[k]
                    for i, annotation in enumerate(resolved_annotation):
                        LOG.debug("Adding annotation:" + str(annotation))
                        existing_annotations.insert(k + i, annotation)
                    break
                else:
                    k -= 1

        return existing_annotations

    def _relocate_annotation(self, annotations: List, offset: int):
        """Shift the annotated entities by given offset."""
        annotations_copy = deepcopy(annotations)
        for annotation in annotations_copy:
            annotation[END_OFFSET] += offset
            annotation[BEGIN_OFFSET] += offset
        return annotations_copy

    def segment(self, text: str, char_offset=0) -> List[Document]:
        """Segment the text into segments of max_doc_length with overlap_tokens."""
        segments = []
        starting_index = 0
        while len(text[starting_index:].encode()) > self.max_doc_size:
            trimmed_text = self._trim_to_max_bytes(text[starting_index:], self.max_doc_size)
            trimmed_text = self._trim_partial_trailing_word(trimmed_text)
            segments.append(Document(text=trimmed_text, char_offset=char_offset + starting_index))
            starting_index = starting_index + self._find_trailing_overlapping_tokens_start_index(trimmed_text) + 1
        # Add the remaining segment
        if starting_index < len(text) - 1:
            segments.append(Document(text=text[starting_index:], char_offset=char_offset + starting_index))
        return segments

    def de_segment(self, segments: List[Document]) -> Document:
        """
        Merge the segments back into one big text. It also merges back the pii classification result.
        Handles conflicting result on overlapping text between two text segments in the following ways:
        1. For pii classification, the maximum thresholds for an entity amongst the segments is
            updated as the threshold for that entity for the merged document
        2. For pii entity annotations, for a conflicting annotation span a higher priority
            is given to the one with a higher confidence threshold
        """
        merged_text = ""
        pii_classification = {}
        pii_entities = []
        segments.sort(key=lambda x: x.char_offset)
        for segment in segments:
            offset_adjusted_segment = Document(text=segment.text, char_offset=segment.char_offset,
                                               pii_entities=self._relocate_annotation(segment.pii_entities, segment.char_offset),
                                               pii_classification=segment.pii_classification)
            self._merge_classifcation_results(segment, pii_classification)
            self._merge_pii_annotation_results(offset_adjusted_segment, pii_entities)
            merged_text = merged_text + segment.text[len(merged_text) - segment.char_offset:]
        return Document(text=merged_text, char_offset=0, pii_classification=pii_classification, pii_entities=pii_entities)


class Redactor:
    """Handle the logic of redacting discovered pii entities from the given text."""

    def __init__(self, redaction_config: RedactionConfig):
        self.redaction_config = redaction_config

    def redact(self, input_text, entities_list):
        """Redact the pii entities from given text."""
        doc_parts_list = []
        prev_entity = None
        for entity in entities_list:
            if entity[SCORE] < self.redaction_config.confidence_threshold:
                continue
            entity_type = entity[ENTITY_TYPE]
            begin_offset = entity[BEGIN_OFFSET]
            end_offset = entity[END_OFFSET]
            if prev_entity is None:
                doc_parts_list.append(input_text[:begin_offset])
            else:
                doc_parts_list.append(input_text[prev_entity[END_OFFSET]:begin_offset])

            if ALL in self.redaction_config.pii_entity_types or entity_type in self.redaction_config.pii_entity_types:
                # Redact this entity type
                if self.redaction_config.mask_mode == REPLACE_WITH_PII_ENTITY_TYPE:
                    # Replace with PII Entity Type
                    doc_parts_list.append(f"[{entity_type}]")
                else:
                    # Replace with MaskCharacter
                    entity_length = end_offset - begin_offset
                    doc_parts_list.append(self.redaction_config.mask_character * entity_length)
            else:
                # Don't redact this entity type
                doc_parts_list.append(input_text[begin_offset:end_offset])

            prev_entity = entity
        if prev_entity is not None:
            doc_parts_list.append(input_text[prev_entity[END_OFFSET]:])
        else:
            doc_parts_list.append(input_text)
        return ''.join([doc_part for doc_part in doc_parts_list])
