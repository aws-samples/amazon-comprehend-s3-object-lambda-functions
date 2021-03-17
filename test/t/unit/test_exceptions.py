from unittest import TestCase

from constants import CONTENT_LENGTH, RANGE
from exceptions import UnsupportedFileException


class ExceptionsTest(TestCase):
    def test_unsupported_file_format_exception(self):
        try:
            http_headers = {CONTENT_LENGTH: 1234, RANGE: "0-123"}
            raise UnsupportedFileException("Some Random blob".encode('utf-8'), http_headers, "Unsupported file")
        except UnsupportedFileException as exception:
            assert exception.file_content.decode('utf-8') == "Some Random blob"
            assert str(exception) == "Unsupported file"
            assert exception.http_headers == http_headers
