"""Custom defined exceptions."""


class CustomException(Exception):
    """Exceptions which are generated because of non-compliance of business constraints of this lambda."""


class UnsupportedFileException(CustomException):
    """Exception generated when we encounter an unsupported file format. For e.g. an image file."""

    def __init__(self, file_content, http_headers, *args, **kwargs):
        super().__init__(*args)
        self.file_content = file_content
        self.http_headers = http_headers


class FileSizeLimitExceededException(CustomException):
    """
    Exception representing file size beyond the supported limits.
    Files beyond this size are prone to take too long to process causing timeouts.
    """

    pass


class TimeoutException(CustomException):
    """Exception raised when some task is not able to complete within a certain time limit."""

    pass


class InvalidConfigurationException(CustomException):
    """Exception representing an incorrect configuration of the access point such as incorrect function payload structure."""

    def __init__(self, message, *args, **kwargs):
        super().__init__(*args)
        self.message = message


class InvalidRequestException(CustomException):
    """Exception representing an invalid request."""

    def __init__(self, message, *args, **kwargs):
        super().__init__(*args)
        self.message = message


class S3DownloadException(CustomException):
    """Exception representing an error occurring during downloading from the presigned url."""

    def __init__(self, s3_error_code, s3_message, *args, **kwargs):
        super().__init__(*args)
        self.s3_error_code = s3_error_code
        self.s3_message = s3_message


class RestrictedDocumentException(CustomException):
    """Exception representing a restricted document throw when it contains pii."""
