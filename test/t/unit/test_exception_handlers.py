from unittest import TestCase
from unittest.mock import patch, MagicMock

from constants import S3_STATUS_CODES, S3_ERROR_CODES, UNSUPPORTED_FILE_HANDLING_VALID_VALUES
from exception_handlers import ExceptionHandler
from exceptions import UnsupportedFileException, FileSizeLimitExceededException, InvalidConfigurationException, \
    S3DownloadException, RestrictedDocumentException, TimeoutException


class ExceptionHandlerTest(TestCase):

    def setUp(self) -> None:
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()

    @patch("exception_handlers.UNSUPPORTED_FILE_HANDLING", UNSUPPORTED_FILE_HANDLING_VALID_VALUES.PASS)
    def test_unsupported_file_exception_handling_do_not_fail(self):
        s3_client = MagicMock()
        ExceptionHandler(s3_client). \
            handle_exception(UnsupportedFileException(file_content="SomeContent", http_headers={'h1': 'v1'}), "SomeRoute", "SomeToken")
        s3_client.respond_back_with_data.assert_called_once_with("SomeContent", {'h1': 'v1'}, "SomeRoute", "SomeToken")

    @patch("exception_handlers.UNSUPPORTED_FILE_HANDLING", UNSUPPORTED_FILE_HANDLING_VALID_VALUES.FAIL)
    def test_unsupported_file_exception_handling_return_error(self):
        s3_client = MagicMock()
        ExceptionHandler(s3_client). \
            handle_exception(UnsupportedFileException(file_content="SomeContent", http_headers={'h1': 'v1'}), "SomeRoute", "SomeToken")
        s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.BAD_REQUEST_400,
                                                                  S3_ERROR_CODES.UnexpectedContent,
                                                                  "Unsupported file encountered for determining Pii",
                                                                  "SomeRoute", "SomeToken")

    @patch("exception_handlers.UNSUPPORTED_FILE_HANDLING", 'Unknown')
    def test_unsupported_file_exception_handling_return_unknown_error(self):
        s3_client = MagicMock()
        self.assertRaises(Exception, ExceptionHandler(s3_client).handle_exception,
                          UnsupportedFileException(file_content="SomeContent", http_headers={'h1': 'v1'}), "SomeRoute", "SomeToken")

    def test_file_size_limit_exceeded_handler(self):
        s3_client = MagicMock()
        ExceptionHandler(s3_client).handle_exception(FileSizeLimitExceededException(), "SomeRoute", "SomeToken")
        s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.BAD_REQUEST_400,
                                                                  S3_ERROR_CODES.EntityTooLarge,
                                                                  "Size of the requested object exceeds maximum file size supported",
                                                                  "SomeRoute", "SomeToken")

    def test_default_exception_handler(self):
        s3_client = MagicMock()
        ExceptionHandler(s3_client).handle_exception(Exception(), "SomeRoute", "SomeToken")
        s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.INTERNAL_SERVER_ERROR_500,
                                                                  S3_ERROR_CODES.InternalError,
                                                                  "An internal error occurred while processing the file",
                                                                  "SomeRoute", "SomeToken")

    def test_invalid_configuration_exception_handler(self):
        s3_client = MagicMock()
        ExceptionHandler(s3_client).handle_exception(InvalidConfigurationException("Missconfigured knob"),
                                                     "SomeRoute", "SomeToken")
        s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.BAD_REQUEST_400,
                                                                  S3_ERROR_CODES.InvalidRequest,
                                                                  "Lambda function has been incorrectly setup",
                                                                  "SomeRoute", "SomeToken")

    def test_s3_download_exception_handler(self):
        s3_client = MagicMock()
        ExceptionHandler(s3_client).handle_exception(S3DownloadException("InternalError", "Internal Server Error"),
                                                     "SomeRoute", "SomeToken")
        s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.INTERNAL_SERVER_ERROR_500,
                                                                  S3_ERROR_CODES.InternalError,
                                                                  "Internal Server Error",
                                                                  "SomeRoute", "SomeToken")

    def test_restricted_document_exception_handler(self):
        s3_client = MagicMock()
        ExceptionHandler(s3_client).handle_exception(RestrictedDocumentException(),
                                                     "SomeRoute", "SomeToken")
        s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.FORBIDDEN_403,
                                                                  S3_ERROR_CODES.AccessDenied,
                                                                  "Document Contains PII",
                                                                  "SomeRoute", "SomeToken")

    def test_timeout_exception_handler(self):
        s3_client = MagicMock()
        ExceptionHandler(s3_client).handle_exception(TimeoutException(),
                                                     "SomeRoute", "SomeToken")
        s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.BAD_REQUEST_400,
                                                                  S3_ERROR_CODES.RequestTimeout,
                                                                  "Failed to complete document processing within time limit",
                                                                  "SomeRoute", "SomeToken")
