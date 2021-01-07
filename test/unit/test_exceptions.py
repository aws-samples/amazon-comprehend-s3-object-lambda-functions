from unittest import TestCase

from exceptions import UnsupportedFileException


class ExceptionsTest(TestCase):
    def test_unsupported_file_format_exception(self):
        try:
            raise UnsupportedFileException("Some Random blob".encode('utf-8'), "Unsupported file")
        except UnsupportedFileException as exception:
            assert exception.file_content.decode('utf-8') == "Some Random blob"
            assert str(exception) == "Unsupported file"
