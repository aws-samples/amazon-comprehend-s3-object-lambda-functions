"""Client wrapper over aws services."""

from typing import Tuple

import boto3
import botocore
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import re

import lambdalogging
from clients.cloudwatch_client import Metrics
from config import DEFAULT_MAX_DOC_SIZE
from constants import CONTENT_LENGTH, S3_STATUS_CODES, S3_ERROR_CODES, error_code_to_enums, WRITE_GET_OBJECT_RESPONSE, \
    DOWNLOAD_PRESIGNED_URL, S3_MAX_RETRIES, S3
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

    def __init__(self, max_file_supported=DEFAULT_MAX_DOC_SIZE):
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

        self.download_metrics = Metrics(service_name=S3, api=DOWNLOAD_PRESIGNED_URL)
        self.write_get_object_metrics = Metrics(service_name=S3, api=WRITE_GET_OBJECT_RESPONSE)

        self.metrics = []

    def _contains_error(self, response) -> Tuple[bool, Tuple[str, str]]:
        text = response.content.decode('utf-8')
        lines = text.split('\n')
        if response.status_code != 200 or (len(lines) > 0 and lines[0] == self.XML_HEADER):
            xml = ''.join(lines[1:])
            LOG.info('Response text contains xml.')
            error_match = re.search(self.ERROR_RE, xml)
            code_match = re.search(self.CODE_RE, xml)
            message_match = re.search(self.MESSAGE_RE, xml)
            if error_match and code_match and message_match:
                error_code = code_match[0]
                error_message = message_match[0]
                return True, (error_code, error_message)
            elif response.status_code != 200:
                return True, (S3_ERROR_CODES.InternalError.name, "Internal Server Error")
        return False, ('', '')

    def download_file_from_presigned_url(self, presigned_url) -> str:
        """
        Download the file from a s3's presigned url.
        Python AWS-SDK doesn't provide any method to download from a presigned url directly so we'd have to make a simple GET httpcall.
        """
        for i in range(self.S3_DOWNLOAD_MAX_RETRIES):
            start_time = time.time()
            response = self.session.get(presigned_url, timeout=self.MAX_GET_TIMEOUT)
            end_time = time.time()
            try:
                # Since presigned urls do not return correct status codes when there is an error,
                # the xml must be parsed to find the error code and status
                error_detected, (error_code, error_message) = self._contains_error(response)
                if error_detected:
                    status_code_enum, error_code_enum = error_code_to_enums(error_code)
                    LOG.error(f"Error downloading file from presigned url. ({error_code}: {error_message})")
                    status_code = int(status_code_enum.name[-3:])
                    if status_code not in self.S3_RETRY_STATUS_CODES or i == self.S3_DOWNLOAD_MAX_RETRIES - 1:
                        LOG.error("Client error or max retries reached for downloading file from presigned url.")
                        self.download_metrics.add_fault_count()
                        raise S3DownloadException(error_code, error_message)
                else:
                    if CONTENT_LENGTH in response.headers and int(response.headers.get(CONTENT_LENGTH)) > self.max_file_supported:
                        raise FileSizeLimitExceededException("File too large to process")
                    self.download_metrics.add_latency(start_time, end_time)
                    return response.content.decode('utf-8')
                time.sleep(max(1.0, i ** self.BACKOFF_FACTOR))
            except UnicodeDecodeError:
                raise UnsupportedFileException(response.content, "Not a valid utf-8 file")

    def respond_back_with_data(self, data, request_route: str, request_token: str):
        """Call S3's WriteGetObjectResponse API to return the processed object back to the original caller of get_object API."""
        start_time = time.time()
        try:
            self.s3.write_get_object_response(Body=data, RequestRoute=request_route, RequestToken=request_token)
            self.write_get_object_metrics.add_latency(start_time, time.time())
        except Exception as error:
            LOG.error("Error occurred while calling s3 write get object response with data.", exc_info=True)
            self.write_get_object_metrics.add_fault_count()
            raise error

    def respond_back_with_error(self, status_code: S3_STATUS_CODES, error_code: S3_ERROR_CODES, error_message: str,
                                request_route: str, request_token: str):
        """Call S3's WriteGetObjectResponse API to return an error to the original caller of get_object API."""
        start_time = time.time()
        try:
            self.s3.write_get_object_response(StatusCode=status_code.name, ErrorCode=error_code.name, ErrorMessage=error_message,
                                              RequestRoute=request_route, RequestToken=request_token)
            self.write_get_object_metrics.add_latency(start_time, time.time())
        except Exception as error:
            LOG.error("Error occurred while calling s3 write get object response with error.", exc_info=True)
            self.write_get_object_metrics.add_fault_count()
            raise error
