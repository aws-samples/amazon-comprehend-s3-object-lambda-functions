"""Collection of constants used in the code."""
from enum import Enum, auto

from typing import Tuple

ENTITY_TYPE = "Type"
BEGIN_OFFSET = "BeginOffset"
END_OFFSET = "EndOffset"
NAME = "Name"
SCORE = "Score"
ALL = "ALL"
REPLACE_WITH_PII_ENTITY_TYPE = "REPLACE_WITH_PII_ENTITY_TYPE"
MASK = "MASK"
REQUEST_ID = "xAmzRequestId"
USER_REQUEST = "userRequest"
HEADERS = "headers"
RANGE = "Range"
PART_NUMBER = "PartNumber"
REQUEST_ROUTE = "outputRoute"
REQUEST_TOKEN = "outputToken"
GET_OBJECT_CONTEXT = "getObjectContext"
INPUT_S3_URL = "inputS3Url"
S3OL_CONFIGURATION = "configuration"
S3OL_ACCESS_POINT_ARN = "accessPointArn"
CONTENT_LENGTH = "Content-Length"
OVERLAP_TOKENS = "overlap_tokens"
PAYLOAD = "payload"
ONE_DOC_PER_LINE = "ONE_DOC_PER_LINE"
ONE_DOC_PER_FILE = "ONE_DOC_PER_FILE"
LANGUAGE_CODE = "language_code"

DEFAULT_USER_AGENT = "S3ObjectLambda/1.0"

RESERVED_TIME_FOR_CLEANUP = 2000   # We need at least this much time (in millis) to perform cleanup tasks like flushing the metrics
COMPREHEND_MAX_RETRIES = 7
S3_MAX_RETRIES = 10
CLOUD_WATCH_NAMESPACE = "ComprehendS3ObjectLambda"
LATENCY = "Latency"
ERROR_COUNT = "ErrorCount"
API = "API"
CONTAINS_PII_ENTITIES = "ContainsPiiEntities"
DETECT_PII_ENTITIES = "DetectPiiEntities"
PII_DOCUMENTS_PROCESSED = "PiiDocumentsProcessed"
DOCUMENTS_PROCESSED = "DocumentsProcessed"
PII_DOCUMENT_TYPES_PROCESSED = "PiiDocumentTypesProcessed"
PII_ENTITY_TYPE = "PiiEntityType"
SERVICE = "Service"
COMPREHEND = "Comprehend"
S3 = "S3"
WRITE_GET_OBJECT_RESPONSE = "WriteGetObjectResponse"
DOWNLOAD_PRESIGNED_URL = "DownloadPresignedUrl"
LANGUAGE = "Language"
MILLISECONDS = "Milliseconds"
COUNT = "Count"
VALUE = "Value"
S3OL_ACCESS_POINT = "S3ObjectLambdaAccessPoint"
METRIC_NAME = "MetricName"
UNIT = "Unit"
DIMENSIONS = "Dimensions"


class UNSUPPORTED_FILE_HANDLING_VALID_VALUES(Enum):
    """Valid values for handling logic for Unsupported files."""

    PASS = auto()
    FAIL = auto()


class MASK_MODE_VALID_VALUES(Enum):
    """Valid values for MASK_MODE variable."""

    MASK = auto()
    REPLACE_WITH_PII_ENTITY_TYPE = auto()


class S3_STATUS_CODES(Enum):
    """
    Valid http status codes for S3.
    Refer https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList for more details on the status codes.
    """

    OK_200 = auto()
    PARTIAL_CONTENT_206 = auto()
    NOT_MODIFIED_304 = auto()
    BAD_REQUEST_400 = auto()
    UNAUTHORIZED_401 = auto()
    FORBIDDEN_403 = auto()
    NOT_FOUND_404 = auto()
    METHOD_NOT_ALLOWED_405 = auto()
    CONFLICT_409 = auto()
    LENGTH_REQUIRED_411 = auto()
    PRECONDITION_FAILED_412 = auto()
    RANGE_NOT_SATISFIABLE_416 = auto()
    INTERNAL_SERVER_ERROR_500 = auto()
    SERVICE_UNAVAILABLE_503 = auto()

    def get_http_status_code(self) -> int:
        """Convert s3 status codes to integer http status codes."""
        return int(self.name.split('_')[-1])


class S3_ERROR_CODES(Enum):
    """
    Valid error codes for S3.
    Refer https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList for more details on error code.
    """

    AccessDenied = auto()
    AccountProblem = auto()
    AllAccessDisabled = auto()
    AmbiguousGrantByEmailAddress = auto()
    AuthorizationHeaderMalformed = auto()
    BadDigest = auto()
    BucketAlreadyExists = auto()
    BucketAlreadyOwnedByYou = auto()
    BucketNotEmpty = auto()
    CredentialsNotSupported = auto()
    CrossLocationLoggingProhibited = auto()
    EntityTooSmall = auto()
    EntityTooLarge = auto()
    ExpiredToken = auto()
    IllegalLocationConstraintException = auto()
    IllegalVersioningConfigurationException = auto()
    IncompleteBody = auto()
    IncorrectNumberOfFilesInPostRequest = auto()
    InlineDataTooLarge = auto()
    InternalError = auto()
    InvalidAccessKeyId = auto()
    InvalidAccessPoint = auto()
    InvalidAddressingHeader = auto()
    InvalidArgument = auto()
    InvalidBucketName = auto()
    InvalidBucketState = auto()
    InvalidDigest = auto()
    InvalidEncryptionAlgorithmError = auto()
    InvalidLocationConstraint = auto()
    InvalidObjectState = auto()
    InvalidPart = auto()
    InvalidPartOrder = auto()
    InvalidPayer = auto()
    InvalidPolicyDocument = auto()
    InvalidRange = auto()
    InvalidRequest = auto()
    InvalidSecurity = auto()
    InvalidSOAPRequest = auto()
    InvalidStorageClass = auto()
    InvalidTargetBucketForLogging = auto()
    InvalidToken = auto()
    InvalidURI = auto()
    KeyTooLongError = auto()
    MalformedACLError = auto()
    MalformedPOSTRequest = auto()
    MalformedXML = auto()
    MaxMessageLengthExceeded = auto()
    MaxPostPreDataLengthExceededError = auto()
    MetadataTooLarge = auto()
    MethodNotAllowed = auto()
    MissingAttachment = auto()
    MissingContentLength = auto()
    MissingRequestBodyError = auto()
    MissingSecurityElement = auto()
    MissingSecurityHeader = auto()
    NoLoggingStatusForKey = auto()
    NoSuchBucket = auto()
    NoSuchBucketPolicy = auto()
    NoSuchKey = auto()
    NoSuchLifecycleConfiguration = auto()
    NoSuchUpload = auto()
    NoSuchVersion = auto()
    NotImplemented = auto()
    NotSignedUp = auto()
    OperationAborted = auto()
    PermanentRedirect = auto()
    PreconditionFailed = auto()
    Redirect = auto()
    RestoreAlreadyInProgress = auto()
    RequestIsNotMultiPartContent = auto()
    RequestTimeout = auto()
    RequestTimeTooSkewed = auto()
    RequestTorrentOfBucketError = auto()
    ServerSideEncryptionConfigurationNotFoundError = auto()
    ServiceUnavailable = auto()
    SignatureDoesNotMatch = auto()
    SlowDown = auto()
    TemporaryRedirect = auto()
    TokenRefreshRequired = auto()
    TooManyAccessPoints = auto()
    TooManyBuckets = auto()
    UnexpectedContent = auto()
    UnresolvableGrantByEmailAddress = auto()
    UserKeyMustBeSpecified = auto()
    NoSuchAccessPoint = auto()
    InvalidTag = auto()
    MalformedPolicy = auto()


ERROR_CODE_STATUS_MAP = {
    S3_ERROR_CODES.AccessDenied: S3_STATUS_CODES.FORBIDDEN_403,
    S3_ERROR_CODES.AccountProblem: S3_STATUS_CODES.FORBIDDEN_403,
    S3_ERROR_CODES.AllAccessDisabled: S3_STATUS_CODES.FORBIDDEN_403,
    S3_ERROR_CODES.AmbiguousGrantByEmailAddress: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.AuthorizationHeaderMalformed: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.BadDigest: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.BucketAlreadyExists: S3_STATUS_CODES.CONFLICT_409,
    S3_ERROR_CODES.BucketAlreadyOwnedByYou: S3_STATUS_CODES.CONFLICT_409,
    S3_ERROR_CODES.BucketNotEmpty: S3_STATUS_CODES.CONFLICT_409,
    S3_ERROR_CODES.CredentialsNotSupported: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.CrossLocationLoggingProhibited: S3_STATUS_CODES.FORBIDDEN_403,
    S3_ERROR_CODES.EntityTooSmall: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.EntityTooLarge: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.ExpiredToken: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.IllegalLocationConstraintException: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.IllegalVersioningConfigurationException: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.IncompleteBody: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.IncorrectNumberOfFilesInPostRequest: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InlineDataTooLarge: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InternalError: S3_STATUS_CODES.INTERNAL_SERVER_ERROR_500,
    S3_ERROR_CODES.InvalidAccessKeyId: S3_STATUS_CODES.FORBIDDEN_403,
    S3_ERROR_CODES.InvalidAccessPoint: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InvalidArgument: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InvalidBucketName: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InvalidBucketState: S3_STATUS_CODES.CONFLICT_409,
    S3_ERROR_CODES.InvalidDigest: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InvalidEncryptionAlgorithmError: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InvalidLocationConstraint: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InvalidObjectState: S3_STATUS_CODES.FORBIDDEN_403,
    S3_ERROR_CODES.InvalidPart: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InvalidPartOrder: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InvalidPayer: S3_STATUS_CODES.FORBIDDEN_403,
    S3_ERROR_CODES.InvalidPolicyDocument: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InvalidRange: S3_STATUS_CODES.RANGE_NOT_SATISFIABLE_416,
    S3_ERROR_CODES.InvalidRequest: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InvalidSecurity: S3_STATUS_CODES.FORBIDDEN_403,
    S3_ERROR_CODES.InvalidSOAPRequest: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InvalidStorageClass: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InvalidTargetBucketForLogging: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InvalidToken: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.InvalidURI: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.KeyTooLongError: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.MalformedACLError: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.MalformedPOSTRequest: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.MalformedXML: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.MaxMessageLengthExceeded: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.MaxPostPreDataLengthExceededError: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.MetadataTooLarge: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.MethodNotAllowed: S3_STATUS_CODES.METHOD_NOT_ALLOWED_405,
    S3_ERROR_CODES.MissingContentLength: S3_STATUS_CODES.LENGTH_REQUIRED_411,
    S3_ERROR_CODES.MissingRequestBodyError: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.MissingSecurityElement: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.MissingSecurityHeader: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.NoLoggingStatusForKey: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.NoSuchBucket: S3_STATUS_CODES.NOT_FOUND_404,
    S3_ERROR_CODES.NoSuchBucketPolicy: S3_STATUS_CODES.NOT_FOUND_404,
    S3_ERROR_CODES.NoSuchKey: S3_STATUS_CODES.NOT_FOUND_404,
    S3_ERROR_CODES.NoSuchLifecycleConfiguration: S3_STATUS_CODES.NOT_FOUND_404,
    S3_ERROR_CODES.NoSuchUpload: S3_STATUS_CODES.NOT_FOUND_404,
    S3_ERROR_CODES.NoSuchVersion: S3_STATUS_CODES.NOT_FOUND_404,
    S3_ERROR_CODES.NotSignedUp: S3_STATUS_CODES.FORBIDDEN_403,
    S3_ERROR_CODES.OperationAborted: S3_STATUS_CODES.CONFLICT_409,
    S3_ERROR_CODES.PreconditionFailed: S3_STATUS_CODES.PRECONDITION_FAILED_412,
    S3_ERROR_CODES.RestoreAlreadyInProgress: S3_STATUS_CODES.CONFLICT_409,
    S3_ERROR_CODES.RequestIsNotMultiPartContent: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.RequestTimeout: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.RequestTimeTooSkewed: S3_STATUS_CODES.FORBIDDEN_403,
    S3_ERROR_CODES.RequestTorrentOfBucketError: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.ServerSideEncryptionConfigurationNotFoundError: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.ServiceUnavailable: S3_STATUS_CODES.SERVICE_UNAVAILABLE_503,
    S3_ERROR_CODES.SignatureDoesNotMatch: S3_STATUS_CODES.FORBIDDEN_403,
    S3_ERROR_CODES.SlowDown: S3_STATUS_CODES.SERVICE_UNAVAILABLE_503,
    S3_ERROR_CODES.TokenRefreshRequired: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.TooManyAccessPoints: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.TooManyBuckets: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.UnexpectedContent: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.UnresolvableGrantByEmailAddress: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.UserKeyMustBeSpecified: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.NoSuchAccessPoint: S3_STATUS_CODES.NOT_FOUND_404,
    S3_ERROR_CODES.InvalidTag: S3_STATUS_CODES.BAD_REQUEST_400,
    S3_ERROR_CODES.MalformedPolicy: S3_STATUS_CODES.BAD_REQUEST_400
}


def error_code_to_enums(error_code: str) -> Tuple[S3_STATUS_CODES, S3_ERROR_CODES]:
    """Error code to enums."""
    for code, status in ERROR_CODE_STATUS_MAP.items():
        if error_code == code.name:
            return status, code
    return S3_STATUS_CODES.INTERNAL_SERVER_ERROR_500, S3_ERROR_CODES.InternalError


def http_status_code_to_s3_status_code(http_status_code: int) -> S3_STATUS_CODES:
    """Convert http status codes to s3 status codes."""
    for status_codes in S3_STATUS_CODES:
        if str(http_status_code) == status_codes.name[-3:]:
            return status_codes
    return S3_STATUS_CODES.INTERNAL_SERVER_ERROR_500
