from unittest import TestCase
from unittest.mock import patch, MagicMock

from clients.cloudwatch_client import CloudWatchClient

BANNER_ACCESS_POINT_TEST = "arn:aws:s3-banner:us-east-1:000000000000:accesspoint/banner"


class CloudWatchClientTest(TestCase):
    @patch('clients.cloudwatch_client.boto3')
    def test_cloudwatch_client_put_pii_document_processed_metric(self, mocked_boto3):
        mocked_client = MagicMock()
        mocked_boto3.client.return_value = mocked_client

        cloudwatch = CloudWatchClient()
        cloudwatch.put_pii_document_processed_metric('en', BANNER_ACCESS_POINT_TEST)

        mocked_client.put_metric_data.assert_called_once()

    @patch('clients.cloudwatch_client.boto3')
    def test_cloudwatch_client_put_document_processed_metric(self, mocked_boto3):
        mocked_client = MagicMock()
        mocked_boto3.client.return_value = mocked_client

        cloudwatch = CloudWatchClient()
        cloudwatch.put_document_processed_metric('en', BANNER_ACCESS_POINT_TEST)

        mocked_client.put_metric_data.assert_called_once()

    @patch('clients.cloudwatch_client.boto3')
    def test_cloudwatch_client_put_pii_document_types_metric(self, mocked_boto3):
        mocked_client = MagicMock()
        mocked_boto3.client.return_value = mocked_client

        cloudwatch = CloudWatchClient()
        cloudwatch.put_pii_document_types_metric(['SSN', 'PHONE'], 'en', BANNER_ACCESS_POINT_TEST)

        mocked_client.put_metric_data.assert_called_once()
