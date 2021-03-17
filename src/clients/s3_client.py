"""Client wrapper over aws services."""
import re
import time
import urllib
from typing import Tuple

import boto3
import botocore
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import lambdalogging
from clients.cloudwatch_client import Metrics
from config import DOCUMENT_MAX_SIZE
from constants import CONTENT_LENGTH, S3_STATUS_CODES, S3_ERROR_CODES, error_code_to_enums, WRITE_GET_OBJECT_RESPONSE, \
    DOWNLOAD_PRESIGNED_URL, S3_MAX_RETRIES, S3, http_status_code_to_s3_status_code
from exceptions import UnsupportedFileException, FileSizeLimitExceededException, S3DownloadException

LOG = lambdalogging.getLogger(__name__)


class S3Client:
    """Wrapper over s3 client."""

    ERROR_RE = r'(?<=<Error>).*(?=<\/Error>)'
    CODE_RE = r'(?<=<Code>).*(?=<\/Code>)'
    MESSAGE_RE = r'(?<=<Message>).*(?=<\/Message>)'
    XML_HEADER = '<?xml version="1.0" encoding="UTF-8"?>'
    S3_DOWNLOAD_MAX_RETRIES = 5
    S3_RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
    BACKOFF_FACTOR = 1.5
    MAX_GET_TIMEOUT = 10
    # Translation map from response headers of s3's getObject response to S3OL's WriteGetObjectResponse's request headers
    S3GET_TO_WGOR_HEADER_TRANSLATION_MAP = {
        "accept-ranges": ("AcceptRanges", str),
        "Cache-Control": ("CacheControl", str),
        "Content-Disposition": ("ContentDisposition", str),
        "Content-Encoding": ("ContentEncoding", str),
        "Content-Language": ("ContentLanguage", str),
        "Content-Length": ("ContentLength", int),
        "Content-Range": ("ContentRange", str),
        "Content-Type": ("ContentType", str),
        "x-amz-delete-marker": ("DeleteMarker", bool),
        "ETag": ("ETag", str),
        "Expires": ("Expires", str),
        "x-amz-expiration": ("Expiration", str),
        "Last-Modified": ("LastModified", str),
        "x-amz-missing-meta": ("MissingMeta", str),
        "x-amz-meta-": ("Metadata", str),
        "x-amz-object-lock-mode": ("ObjectLockMode", str),
        "x-amz-object-lock-legal-hold": ("ObjectLockLegalHoldStatus", str),
        "x-amz-object-lock-retain-until-date": ("ObjectLockRetainUntilDate", str),
        "x-amz-mp-parts-count": ("PartsCount", int),
        "x-amz-replication-status": ("ReplicationStatus", str),
        "x-amz-request-charged": ("RequestCharged", str),
        "x-amz-restore": ("Restore", str),
        "x-amz-server-side-encryption": ("ServerSideEncryption", str),
        "x-amz-server-side-encryption-customer-algorithm": ("SSECustomerAlgorithm", str),
        "x-amz-server-side-encryption-aws-kms-key-id": ("SSEKMSKeyId", str),
        "x-amz-server-side-encryption-customer-key-MD5": ("SSECustomerKeyMD5", str),
        "x-amz-storage-class": ("StorageClass", str),
        "x-amz-tagging-count": ("TagCount", int),
        "x-amz-version-id": ("VersionId", str),
    }
    # Restricted http headers that can't be sent to s3 as part of downloading object using preseigned url
    # Adding these headers can causes a mismatch with Sigv4 signature
    BLOCKED_REQUEST_HEADERS = ("Host")

    def __init__(self, s3ol_access_point: str, max_file_supported=DOCUMENT_MAX_SIZE):
        self.max_file_supported = max_file_supported
        session_config = botocore.config.Config(
            retries={
                'max_attempts': S3_MAX_RETRIES,
                'mode': 'standard'
            })
        self.s3 = boto3.client('s3', config=session_config)

        self.session = requests.Session()
        self.session.mount("https://", adapter=HTTPAdapter(max_retries=Retry(
            total=self.S3_DOWNLOAD_MAX_RETRIES,
            status_forcelist=self.S3_RETRY_STATUS_CODES,
            method_whitelist=["GET"],
            backoff_factor=self.BACKOFF_FACTOR
        )))

        self.download_metrics = Metrics(service_name=S3, api=DOWNLOAD_PRESIGNED_URL, s3ol_access_point=s3ol_access_point)
        self.write_get_object_metrics = Metrics(service_name=S3, api=WRITE_GET_OBJECT_RESPONSE, s3ol_access_point=s3ol_access_point)

    def _contains_error(self, response) -> Tuple[bool, Tuple[str, str, S3_STATUS_CODES]]:
        text = response.content.decode('utf-8')
        lines = text.split('\n')
        # All 200-299 status codes are succesfull responses . 206 is for partial code .
        if response.status_code >= 300 or (len(lines) > 0 and lines[0] == self.XML_HEADER):
            xml = ''.join(lines[1:])
            LOG.info('Response status code >=300 or text contains xml. ')
            error_match = re.search(self.ERROR_RE, xml)
            code_match = re.search(self.CODE_RE, xml)
            message_match = re.search(self.MESSAGE_RE, xml)
            if error_match and code_match and message_match:
                error_code = code_match[0]
                error_message = message_match[0]
                return True, (error_code, error_message, http_status_code_to_s3_status_code(response.status_code))
            elif response.status_code >= 300:
                return True, (
                    S3_ERROR_CODES.InternalError.name, "Internal Server Error", http_status_code_to_s3_status_code(response.status_code))
        return False, ('', '', http_status_code_to_s3_status_code(response.status_code))

    def _parse_response_headers(self, headers):
        """
        Convert response headers received from s3 presigned download call to the format similar to arguments of WriteGetObjectResponse API.
        :param headers: http headers received as part of response from downloading the object from s3
        """
        transformed_headers = {}
        for header_name in headers:
            if header_name in self.S3GET_TO_WGOR_HEADER_TRANSLATION_MAP:
                header_value = self.S3GET_TO_WGOR_HEADER_TRANSLATION_MAP[header_name][1](headers[header_name])
                transformed_headers[self.S3GET_TO_WGOR_HEADER_TRANSLATION_MAP[header_name][0]] = header_value

        return transformed_headers

    def _filter_request_headers(self, presigned_url, headers={}):
        """
        Filter some restricted headers that shouldn't be passed along to s3 when downloading the object.
        :param headers: http header from the incoming request
        :return: a filtered list of headers
        """
        filtered_headers = {}
        parsed_url = urllib.parse.urlparse(presigned_url)
        parsed_query_params = urllib.parse.parse_qs(parsed_url.query)
        signed_headers = set(parsed_query_params.get('X-Amz-SignedHeaders', []))

        for header in headers:
            if header in self.BLOCKED_REQUEST_HEADERS:
                continue
            if str(header).lower().startswith('x-amz-') and header not in signed_headers:
                continue
            filtered_headers[header] = headers[header]
        return filtered_headers

    def download_file_from_presigned_url(self, presigned_url, headers=None) -> Tuple[str, map, S3_STATUS_CODES]:
        """
        Download the file from a s3's presigned url.
        Python AWS-SDK doesn't provide any method to download from a presigned url directly so we'd have to make a simple GET httpcall.
        """
        parsed_headers = self._filter_request_headers(presigned_url, headers)
        for i in range(self.S3_DOWNLOAD_MAX_RETRIES):
            start_time = time.time()
            LOG.debug(f"Downloading object with presigned url {presigned_url} and headers: {parsed_headers}")
            response = self.session.get(presigned_url, timeout=self.MAX_GET_TIMEOUT, headers=parsed_headers)
            end_time = time.time()
            try:
                # Since presigned urls do not return correct status codes when there is an error,
                # the xml must be parsed to find the error code and status
                error_detected, (error_code, error_message, response_status_code) = self._contains_error(response)
                if error_detected:
                    status_code_enum, error_code_enum = error_code_to_enums(error_code)
                    LOG.error(f"Error downloading file from presigned url. ({error_code}: {error_message})")
                    status_code = int(status_code_enum.name[-3:])
                    if status_code not in self.S3_RETRY_STATUS_CODES or i == self.S3_DOWNLOAD_MAX_RETRIES - 1:
                        LOG.error("Client error or max retries reached for downloading file from presigned url.")
                        self.download_metrics.add_fault_count()
                        raise S3DownloadException(error_code, error_message)
                else:
                    text_content = response.content.decode('utf-8')
                    if CONTENT_LENGTH in response.headers and int(response.headers.get(CONTENT_LENGTH)) > self.max_file_supported:
                        raise FileSizeLimitExceededException("File too large to process")
                    self.download_metrics.add_latency(start_time, end_time)
                    return text_content, response.headers, response_status_code,
                time.sleep(max(1.0, i ** self.BACKOFF_FACTOR))
            except UnicodeDecodeError:
                raise UnsupportedFileException(response.content, response.headers, "Not a valid utf-8 file")

    def respond_back_with_data(self, data, headers: map, request_route: str, request_token: str,
                               status_code: S3_STATUS_CODES = S3_STATUS_CODES.OK_200):
        """Call S3's WriteGetObjectResponse API to return the processed object back to the original caller of get_object API."""
        start_time = time.time()
        try:
            parsed_headers = self._parse_response_headers(headers)
            LOG.debug(f"Calling s3 WriteGetObjectResponse with RequestRoute:{request_route} , headers: {parsed_headers},"
                      f" RequestToken: {request_token}")
            self.s3.write_get_object_response(StatusCode=status_code.get_http_status_code(), Body=data, RequestRoute=request_route,
                                              RequestToken=request_token, **parsed_headers)
        except Exception as error:
            LOG.error("Error occurred while calling s3 write get object response with data.", exc_info=True)
            self.write_get_object_metrics.add_fault_count()
            raise error
        finally:
            self.write_get_object_metrics.add_latency(start_time, time.time())

    def respond_back_with_error(self, status_code: S3_STATUS_CODES, error_code: S3_ERROR_CODES, error_message: str,
                                request_route: str, request_token: str):
        """Call S3's WriteGetObjectResponse API to return an error to the original caller of get_object API."""
        start_time = time.time()
        try:
            self.s3.write_get_object_response(StatusCode=status_code.get_http_status_code(), ErrorCode=error_code.name,
                                              ErrorMessage=error_message,
                                              RequestRoute=request_route, RequestToken=request_token)
        except Exception as error:
            LOG.error("Error occurred while calling s3 write get object response with error.", exc_info=True)
            self.write_get_object_metrics.add_fault_count()
            raise error
        finally:
            self.write_get_object_metrics.add_latency(start_time, time.time())
