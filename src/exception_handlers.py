"""Classes for handling exceptions."""
import lambdalogging
from clients.s3_client import S3Client
from config import UNSUPPORTED_FILE_HANDLING
from constants import UNSUPPORTED_FILE_HANDLING_VALID_VALUES, S3_STATUS_CODES, S3_ERROR_CODES, error_code_to_enums
from exceptions import UnsupportedFileException, FileSizeLimitExceededException, S3DownloadException

LOG = lambdalogging.getLogger(__name__)


class ExceptionHandler:
    """Handler enclosing an action to be taken in case of an error occurred while processing files."""

    def handle_exception(self, exception: BaseException, request_route: str, request_token: str, **kwargs):
        """Handle exception and take appropriate actions."""
        raise NotImplementedError


class UnsupportedFileExceptionHandler(ExceptionHandler):
    """Handles the action to be taken in case we encounter a file which is not supported by Lambda's core functionality."""

    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client

    def handle_exception(self, exception: UnsupportedFileException, request_route: str, request_token: str, **kwargs):
        """
        Handle the FileUnsupportedException.
        :param exception: the exception which occurred
        :param file_content: content of the file for which we got the error
        :param kwargs: kwargs
        """""
        LOG.debug("File is not supported for determining and redacting pii data.")

        if UNSUPPORTED_FILE_HANDLING == UNSUPPORTED_FILE_HANDLING_VALID_VALUES.PASS:
            LOG.debug("Unsupported file handling strategy is set to PASS. Responding back with the file content to the caller")
            self.s3_client.respond_back_with_data(exception.file_content, request_route, request_token)

        elif UNSUPPORTED_FILE_HANDLING == UNSUPPORTED_FILE_HANDLING_VALID_VALUES.FAIL:
            LOG.debug(
                f"Unsupported file handling strategy is set to FAIL. Responding back with error: "
                f"{S3_ERROR_CODES.PreconditionFailed.name} to the caller")
            self.s3_client.respond_back_with_error(S3_STATUS_CODES.PRECONDITION_FAILED_412,
                                                   S3_ERROR_CODES.PreconditionFailed,
                                                   "Unsupported file encountered for determining Pii", request_route, request_token)
        else:
            raise Exception("Unknown exception handling strategy found for UnsupportedFileException.")


class FileSizeLimitExceededExceptionHandler(ExceptionHandler):
    """Handle the action to be taken when size of the requested object exceeds the maximum size supported."""

    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client

    def handle_exception(self, exception: FileSizeLimitExceededException, request_route: str, request_token: str, **kwargs):
        """Handle the FileSizeLimitExceededException."""
        LOG.info(
            f"File size of the requested object exceeds maximum file size supported. Responding back with"
            f"error: {S3_STATUS_CODES.PRECONDITION_FAILED_412.name} ")
        self.s3_client.respond_back_with_error(S3_STATUS_CODES.PRECONDITION_FAILED_412,
                                               S3_ERROR_CODES.EntityTooLarge,
                                               "Size of the requested object exceeds maximum file size supported", request_route,
                                               request_token)


class DefaultExceptionHandler(ExceptionHandler):
    """Default handler for Exceptions generated while processing the file."""

    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client

    def handle_exception(self, exception: Exception, request_route: str, request_token: str, **kwargs):
        """Handle the exception by returning back a 5xx error to the caller."""
        LOG.error("Internal error occurred while processing the file", exc_info=True)
        self.s3_client.respond_back_with_error(S3_STATUS_CODES.INTERNAL_SERVER_ERROR_500,
                                               S3_ERROR_CODES.InternalError,
                                               "An internal error occurred while processing the file", request_route,
                                               request_token)


class InvalidConfigurationExceptionHandler(ExceptionHandler):
    """Handle exception from an incorrect configuration which restricts lambda function to even start handling the incoming events."""

    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client

    def handle_exception(self, exception: Exception, request_route: str, request_token: str, **kwargs):
        """Handle the exception by returning back a 4xx error to the caller."""
        LOG.error(f"Encountered an invalid configuration setup. {exception}", exc_info=True)
        self.s3_client.respond_back_with_error(S3_STATUS_CODES.BAD_REQUEST_400,
                                               S3_ERROR_CODES.InvalidRequest,
                                               "Lambda function has been incorrectly setup", request_route,
                                               request_token)


class S3DownloadExceptionHandler(ExceptionHandler):
    """Handle exception thrown when there is an error downloading from the presigned url."""

    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client

    def handle_exception(self, exception: S3DownloadException, request_route: str, request_token: str, **kwargs):
        """Handle the exception by returning back a 4xx error to the caller."""
        LOG.error(f"Error downloading from presigned url. {exception}", exc_info=True)
        status_code, error_code = error_code_to_enums(exception.s3_code)
        self.s3_client.respond_back_with_error(status_code, error_code, exception.s3_message,
                                               request_route, request_token)


class RestrictedDocumentExceptionHandler(ExceptionHandler):
    """Handle exception thrown when a document contains pii."""

    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client

    def handle_exception(self, exception: Exception, request_route: str, request_token: str, **kwargs):
        """Handle the exception by returning back a 4xx error to the caller."""
        LOG.error(f"Document contains pii. {exception}", exc_info=True)
        self.s3_client.respond_back_with_error(S3_STATUS_CODES.FORBIDDEN_403,
                                               S3_ERROR_CODES.AccessDenied,
                                               "Document Contains PII",
                                               request_route, request_token)
