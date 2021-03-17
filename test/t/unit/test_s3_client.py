from unittest import TestCase
from unittest.mock import patch, MagicMock

from clients.s3_client import S3Client
from constants import BEGIN_OFFSET, END_OFFSET, ENTITY_TYPE, SCORE, S3_STATUS_CODES, S3_ERROR_CODES, RANGE
from exceptions import S3DownloadException, FileSizeLimitExceededException, UnsupportedFileException

PRESIGNED_URL_TEST = "https://s3ol-classifier.s3.amazonaws.com/test.txt"


class MockResponse:
    def __init__(self, content, status_code, headers):
        self.status_code = status_code
        self.content = content
        self.headers = headers


def get_s3_xml_response(code: str, message: str = '') -> str:
    return f"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code><Message>{message}</Message></Error>"


class S3ClientTest(TestCase):
    @patch('clients.s3_client.boto3')
    def test_s3_client_respond_back_with_error(self, mocked_boto3):
        mocked_client = MagicMock()
        mocked_boto3.client.return_value = mocked_client
        s3_client = S3Client(s3ol_access_point="Random_access_point")
        s3_client.respond_back_with_error(status_code=S3_STATUS_CODES.PRECONDITION_FAILED_412,
                                          error_code=S3_ERROR_CODES.PreconditionFailed, error_message="Some Error",
                                          request_route="Route", request_token="q2334")

        mocked_client.write_get_object_response.assert_called_once_with(StatusCode=412,
                                                                        ErrorCode='PreconditionFailed',
                                                                        ErrorMessage="Some Error",
                                                                        RequestRoute='Route', RequestToken="q2334")

    @patch('clients.s3_client.boto3')
    def test_s3_client_respond_back_with_data_default_status_code(self, mocked_boto3):
        mocked_client = MagicMock()
        mocked_boto3.client.return_value = mocked_client
        s3_client = S3Client(s3ol_access_point="Random_access_point")
        s3_client.respond_back_with_data(data='SomeData',
                                         headers={"ContentRange": "0-100", "SomeRandomHeader": '0123', "Content-Length": "101"},
                                         request_route="Route", request_token="q2334")

        mocked_client.write_get_object_response.assert_called_once_with(Body='SomeData', ContentLength=101,
                                                                        RequestRoute='Route', RequestToken="q2334",
                                                                        StatusCode=200)

    @patch('clients.s3_client.boto3')
    def test_s3_client_respond_back_with_data_partial_data(self, mocked_boto3):
        mocked_client = MagicMock()
        mocked_boto3.client.return_value = mocked_client
        s3_client = S3Client(s3ol_access_point="Random_access_point")
        s3_client.respond_back_with_data(data='SomeData', headers={"Content-Range": "0-1200", "SomeRandomHeader": '0123'},
                                         request_route="Route", request_token="q2334", status_code=S3_STATUS_CODES.PARTIAL_CONTENT_206)

        mocked_client.write_get_object_response.assert_called_once_with(Body='SomeData', ContentRange="0-1200",
                                                                        RequestRoute='Route', RequestToken="q2334",
                                                                        StatusCode=206)

    @patch('clients.s3_client.requests.Session.get',
           side_effect=lambda *args, **kwargs: MockResponse(b'Test', 200, {'Content-Length': '4'}))
    def test_s3_client_download_file_from_presigned_url_200_ok(self, mocked_get):
        s3_client = S3Client(s3ol_access_point="Random_access_point")
        http_header = {'some-header': 'header-value'}
        text, response_http_headers, status_code = s3_client.download_file_from_presigned_url(PRESIGNED_URL_TEST, http_header)
        assert text == 'Test'
        assert response_http_headers == {'Content-Length': '4'}
        assert status_code == S3_STATUS_CODES.OK_200
        mocked_get.assert_called_with(PRESIGNED_URL_TEST, timeout=10, headers=http_header)

    @patch('clients.s3_client.requests.Session.get',
           side_effect=lambda *args, **kwargs: MockResponse(b'Test', 206, {'Content-Length': '100'}))
    def test_s3_client_download_partial_file_from_presigned_url(self, mocked_get):
        s3_client = S3Client(s3ol_access_point="Random_access_point")
        http_header = {'some-header': 'header-value'}
        text, response_http_headers, status_code = s3_client.download_file_from_presigned_url(PRESIGNED_URL_TEST, http_header)
        assert text == 'Test'
        assert response_http_headers == {'Content-Length': '100'}
        assert status_code == S3_STATUS_CODES.PARTIAL_CONTENT_206
        mocked_get.assert_called_with(PRESIGNED_URL_TEST, timeout=10, headers=http_header)

    @patch('clients.s3_client.requests.Session.get',
           side_effect=lambda *args, **kwargs: MockResponse(b'Test', 400, {'Content-Length': '4'}))
    def test_s3_client_download_file_from_presigned_url_400_from_get(self, mocked_get):
        s3_client = S3Client(s3ol_access_point="Random_access_point")
        self.assertRaises(S3DownloadException, s3_client.download_file_from_presigned_url, PRESIGNED_URL_TEST, {})

        assert mocked_get.call_count == 5

    @patch('clients.s3_client.requests.Session.get',
           side_effect=lambda *args, **kwargs: MockResponse(b'A' * (11 * 1024 * 1024), 200, {'Content-Length': str(11 * 1024 * 1024)}))
    def test_s3_client_download_file_from_presigned_url_file_size_limit_exceeded(self, mocked_get):
        s3_client = S3Client(s3ol_access_point="Random_access_point")
        self.assertRaises(FileSizeLimitExceededException, s3_client.download_file_from_presigned_url, PRESIGNED_URL_TEST, {})

        mocked_get.assert_called_once()

    @patch('clients.s3_client.requests.Session.get',
           side_effect=lambda *args, **kwargs: MockResponse(get_s3_xml_response('AccessDenied').encode('utf-8'), 200,
                                                            {'Content-Length': '4'}))
    def test_s3_client_download_file_from_presigned_url_access_denied(self, mocked_get):
        s3_client = S3Client(s3ol_access_point="Random_access_point")
        self.assertRaises(S3DownloadException, s3_client.download_file_from_presigned_url, PRESIGNED_URL_TEST, {})

        mocked_get.assert_called_once()

    @patch('clients.s3_client.requests.Session.get',
           side_effect=lambda *args, **kwargs: MockResponse(get_s3_xml_response('UnknownError').encode('utf-8'), 200,
                                                            {'Content-Length': '4'}))
    def test_s3_client_download_file_from_presigned_url_unknown_error(self, mocked_get):
        s3_client = S3Client(s3ol_access_point="Random_access_point")
        self.assertRaises(S3DownloadException, s3_client.download_file_from_presigned_url, PRESIGNED_URL_TEST, {})

        assert mocked_get.call_count == 5

    @patch('clients.s3_client.requests.Session.get',
           side_effect=lambda *args, **kwargs: MockResponse(bytearray.fromhex('ff'), 200, {'Content-Length': '4'}))
    def test_s3_client_download_file_from_presigned_unicode_decode_error(self, mocked_get):
        s3_client = S3Client(s3ol_access_point="Random_access_point")
        self.assertRaises(UnsupportedFileException, s3_client.download_file_from_presigned_url, PRESIGNED_URL_TEST, {})

        mocked_get.assert_called_once()

    @patch('clients.s3_client.requests.Session.get',
           side_effect=lambda *args, **kwargs: MockResponse(bytearray.fromhex('ff'), 200, {'Content-Length': '4'}))
    def test_s3_client_download_file_from_presigned_unicode_decode_error_error(self, mocked_get):
        s3_client = S3Client(s3ol_access_point="Random_access_point")
        self.assertRaises(UnsupportedFileException, s3_client.download_file_from_presigned_url, PRESIGNED_URL_TEST, {})

        mocked_get.assert_called_once()

    @patch('clients.s3_client.requests.Session.get',
           side_effect=lambda *args, **kwargs: MockResponse(get_s3_xml_response('InternalError').encode('utf-8'), 200,
                                                            {'Content-Length': '4'}))
    def test_s3_client_download_file_from_presigned_retry(self, mocked_get):
        s3_client = S3Client(s3ol_access_point="Random_access_point")
        self.assertRaises(S3DownloadException, s3_client.download_file_from_presigned_url, PRESIGNED_URL_TEST, {})

        assert mocked_get.call_count == 5
