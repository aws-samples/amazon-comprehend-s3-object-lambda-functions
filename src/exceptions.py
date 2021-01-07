"""Custom defined exceptions."""


class CustomException(Exception):
    """Exceptions which are generated because of non-compliance of business constraints of this lambda."""


class UnsupportedFileException(CustomException):
    """Exception generated when we encounter an unsupported file format. For e.g. an image file."""

    def __init__(self, file_content, *args, **kwargs):
        super().__init__(*args)
        self.file_content = file_content


class FileSizeLimitExceededException(CustomException):
    """
    Exception representing file size beyond the supported limits.
    Files beyond this size are prone to take too long to process causing timeouts.
    """

    pass


class InvalidConfigurationException(CustomException):
    """Exception representing an incorrect configuration of the access point such as incorrect function payload structure."""

    pass


class S3DownloadException(CustomException):
    """Exception representing an error occurring during downloading from the presigned url."""

    def __init__(self, s3_code, s3_message, *args, **kwargs):
        super().__init__(*args)
        self.s3_code = s3_code
        self.s3_message = s3_message


class RestrictedDocumentException(CustomException):
    """Exception representing a restricted document throw when it contains pii."""
