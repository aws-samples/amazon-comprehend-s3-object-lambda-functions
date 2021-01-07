from unittest import TestCase
from unittest.mock import patch, MagicMock

from exception_handlers import ExceptionHandler, FileSizeLimitExceededExceptionHandler, UnsupportedFileExceptionHandler, \
    DefaultExceptionHandler, InvalidConfigurationExceptionHandler, S3DownloadExceptionHandler, RestrictedDocumentExceptionHandler
from exceptions import UnsupportedFileException, FileSizeLimitExceededException, InvalidConfigurationException, \
    S3DownloadException, RestrictedDocumentException
from constants import S3_STATUS_CODES, S3_ERROR_CODES, UNSUPPORTED_FILE_HANDLING_VALID_VALUES


class ExceptionHandlerTest(TestCase):

    def setUp(self) -> None:
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()

    @patch("exception_handlers.UNSUPPORTED_FILE_HANDLING", UNSUPPORTED_FILE_HANDLING_VALID_VALUES.PASS)
    def test_unsupported_file_exception_handling_do_not_fail(self):
        s3_client = MagicMock()
        UnsupportedFileExceptionHandler(s3_client). \
            handle_exception(UnsupportedFileException(file_content="SomeContent"), "SomeRoute", "SomeToken")
        s3_client.respond_back_with_data.assert_called_once_with("SomeContent", "SomeRoute", "SomeToken")

    @patch("exception_handlers.UNSUPPORTED_FILE_HANDLING", UNSUPPORTED_FILE_HANDLING_VALID_VALUES.FAIL)
    def test_unsupported_file_exception_handling_return_error(self):
        s3_client = MagicMock()
        UnsupportedFileExceptionHandler(s3_client). \
            handle_exception(UnsupportedFileException(file_content="SomeContent"), "SomeRoute", "SomeToken")
        s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.PRECONDITION_FAILED_412,
                                                                  S3_ERROR_CODES.PreconditionFailed,
                                                                  "Unsupported file encountered for determining Pii",
                                                                  "SomeRoute", "SomeToken")

    @patch("exception_handlers.UNSUPPORTED_FILE_HANDLING", 'Unknown')
    def test_unsupported_file_exception_handling_return_unknown_error(self):
        s3_client = MagicMock()
        self.assertRaises(Exception, UnsupportedFileExceptionHandler(s3_client).handle_exception,
                          UnsupportedFileException(file_content="SomeContent"), "SomeRoute", "SomeToken")

    def test_ExceptionHandler_interface(self):
        try:
            ExceptionHandler().handle_exception(Exception(), "SomeRoute", "SomeToken")
            assert False, "Expected a NotImplementedError"
        except NotImplementedError:
            return

    def test_file_size_limit_exceeded_handler(self):
        s3_client = MagicMock()
        FileSizeLimitExceededExceptionHandler(s3_client).handle_exception(FileSizeLimitExceededException(), "SomeRoute", "SomeToken")
        s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.PRECONDITION_FAILED_412,
                                                                  S3_ERROR_CODES.EntityTooLarge,
                                                                  "Size of the requested object exceeds maximum file size supported",
                                                                  "SomeRoute", "SomeToken")

    def test_default_exception_handler(self):
        s3_client = MagicMock()
        DefaultExceptionHandler(s3_client).handle_exception(Exception(), "SomeRoute", "SomeToken")
        s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.INTERNAL_SERVER_ERROR_500,
                                                                  S3_ERROR_CODES.InternalError,
                                                                  "An internal error occurred while processing the file",
                                                                  "SomeRoute", "SomeToken")

    def test_invalid_configuration_exception_handler(self):
        s3_client = MagicMock()
        InvalidConfigurationExceptionHandler(s3_client).handle_exception(InvalidConfigurationException("Missconfigured knob"),
                                                                         "SomeRoute", "SomeToken")
        s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.BAD_REQUEST_400,
                                                                  S3_ERROR_CODES.InvalidRequest,
                                                                  "Lambda function has been incorrectly setup",
                                                                  "SomeRoute", "SomeToken")

    def test_s3_download_exception_handler(self):
        s3_client = MagicMock()
        S3DownloadExceptionHandler(s3_client).handle_exception(S3DownloadException("InternalError", "Internal Server Error"),
                                                               "SomeRoute", "SomeToken")
        s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.INTERNAL_SERVER_ERROR_500,
                                                                  S3_ERROR_CODES.InternalError,
                                                                  "Internal Server Error",
                                                                  "SomeRoute", "SomeToken")

    def test_restricted_document_exception_handler(self):
        s3_client = MagicMock()
        RestrictedDocumentExceptionHandler(s3_client).handle_exception(RestrictedDocumentException(),
                                                                       "SomeRoute", "SomeToken")
        s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.FORBIDDEN_403,
                                                                  S3_ERROR_CODES.AccessDenied,
                                                                  "Document Contains PII",
                                                                  "SomeRoute", "SomeToken")
