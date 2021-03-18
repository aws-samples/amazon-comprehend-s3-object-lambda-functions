"""Classes for handling exceptions."""
import lambdalogging
from clients.s3_client import S3Client
from config import UNSUPPORTED_FILE_HANDLING
from constants import UNSUPPORTED_FILE_HANDLING_VALID_VALUES, S3_STATUS_CODES, S3_ERROR_CODES, error_code_to_enums
from exceptions import UnsupportedFileException, FileSizeLimitExceededException, S3DownloadException, InvalidConfigurationException, \
    InvalidRequestException, RestrictedDocumentException, TimeoutException

LOG = lambdalogging.getLogger(__name__)


class ExceptionHandler:
    """Handler enclosing an action to be taken in case of an error occurred while processing files."""

    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client

    def handle_exception(self, exception: BaseException, request_route: str, request_token: str):
        """Handle exception and take appropriate actions."""
        try:
            raise exception
        except UnsupportedFileException as e:
            self._handle_unsupported_file_exception(e, request_route, request_token)
        except InvalidConfigurationException as e:
            LOG.error(f"Encountered an invalid configuration setup. {e}", exc_info=True)
            self.s3_client.respond_back_with_error(S3_STATUS_CODES.BAD_REQUEST_400,
                                                   S3_ERROR_CODES.InvalidRequest,
                                                   "Lambda function has been incorrectly setup", request_route,
                                                   request_token)
        except FileSizeLimitExceededException:
            LOG.info(
                f"File size of the requested object exceeds maximum file size supported. Responding back with"
                f"error: {S3_STATUS_CODES.BAD_REQUEST_400.name} ")
            self.s3_client.respond_back_with_error(S3_STATUS_CODES.BAD_REQUEST_400, S3_ERROR_CODES.EntityTooLarge,
                                                   "Size of the requested object exceeds maximum file size supported", request_route,
                                                   request_token)
        except InvalidRequestException as e:
            LOG.info(f"Encountered an invalid request {e}", exc_info=True)
            self.s3_client.respond_back_with_error(S3_STATUS_CODES.BAD_REQUEST_400, S3_ERROR_CODES.InvalidRequest,
                                                   e.message, request_route, request_token)
        except S3DownloadException as e:
            LOG.error(f"Error downloading from presigned url. {e}", exc_info=True)
            status_code, error_code = error_code_to_enums(e.s3_error_code)
            self.s3_client.respond_back_with_error(status_code, error_code, e.s3_message,
                                                   request_route, request_token)
        except RestrictedDocumentException as e:
            LOG.error(f"Document contains pii. {e}", exc_info=True)
            self.s3_client.respond_back_with_error(S3_STATUS_CODES.FORBIDDEN_403,
                                                   S3_ERROR_CODES.AccessDenied,
                                                   "Document Contains PII",
                                                   request_route, request_token)
        except TimeoutException as e:
            LOG.error(f"Couldn't complete processing within the time limit. {e}", exc_info=True)
            self.s3_client.respond_back_with_error(S3_STATUS_CODES.BAD_REQUEST_400,
                                                   S3_ERROR_CODES.RequestTimeout,
                                                   "Failed to complete document processing within time limit",
                                                   request_route, request_token)
        except Exception as e:
            LOG.error(f"Internal error {e} occurred while processing the file", exc_info=True)
            self.s3_client.respond_back_with_error(S3_STATUS_CODES.INTERNAL_SERVER_ERROR_500, S3_ERROR_CODES.InternalError,
                                                   "An internal error occurred while processing the file", request_route,
                                                   request_token)

    def _handle_unsupported_file_exception(self, exception: UnsupportedFileException, request_route: str, request_token: str):
        """Handle the action to be taken in case we encounter a file which is not supported by Lambda's core functionality."""
        LOG.debug("File is not supported for determining and redacting pii data.")

        if UNSUPPORTED_FILE_HANDLING == UNSUPPORTED_FILE_HANDLING_VALID_VALUES.PASS:
            LOG.debug("Unsupported file handling strategy is set to PASS. Responding back with the file content to the caller")
            self.s3_client.respond_back_with_data(exception.file_content, exception.http_headers, request_route, request_token)

        elif UNSUPPORTED_FILE_HANDLING == UNSUPPORTED_FILE_HANDLING_VALID_VALUES.FAIL:
            LOG.debug(
                f"Unsupported file handling strategy is set to FAIL. Responding back with error: "
                f"{S3_ERROR_CODES.UnexpectedContent.name} to the caller")
            self.s3_client.respond_back_with_error(S3_STATUS_CODES.BAD_REQUEST_400,
                                                   S3_ERROR_CODES.UnexpectedContent,
                                                   "Unsupported file encountered for determining Pii", request_route, request_token)
        else:
            raise Exception("Unknown exception handling strategy found for UnsupportedFileException.")
