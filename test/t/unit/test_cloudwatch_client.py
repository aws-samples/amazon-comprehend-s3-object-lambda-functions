from unittest import TestCase
from unittest.mock import patch, MagicMock

from clients.cloudwatch_client import CloudWatchClient

S3OL_ACCESS_POINT_TEST = "arn:aws:s3-object-lambda:us-east-1:000000000000:accesspoint/myPiiAp"


class CloudWatchClientTest(TestCase):
    @patch('clients.cloudwatch_client.boto3')
    def test_cloudwatch_client_put_pii_document_processed_metric(self, mocked_boto3):
        mocked_client = MagicMock()
        mocked_boto3.client.return_value = mocked_client

        cloudwatch = CloudWatchClient()
        cloudwatch.put_pii_document_processed_metric('en', S3OL_ACCESS_POINT_TEST)

        mocked_client.put_metric_data.assert_called_once()

    @patch('clients.cloudwatch_client.boto3')
    def test_cloudwatch_client_put_document_processed_metric(self, mocked_boto3):
        mocked_client = MagicMock()
        mocked_boto3.client.return_value = mocked_client

        cloudwatch = CloudWatchClient()
        cloudwatch.put_document_processed_metric('en', S3OL_ACCESS_POINT_TEST)

        mocked_client.put_metric_data.assert_called_once()

    @patch('clients.cloudwatch_client.boto3')
    def test_cloudwatch_client_put_pii_document_types_metric(self, mocked_boto3):
        mocked_client = MagicMock()
        mocked_boto3.client.return_value = mocked_client

        cloudwatch = CloudWatchClient()
        cloudwatch.put_pii_document_types_metric(['SSN', 'PHONE'], 'en', S3OL_ACCESS_POINT_TEST)

        mocked_client.put_metric_data.assert_called_once()

    def test_segment_metrics_segmentation_required(self):
        cloudwatch = CloudWatchClient()
        metric_list = [i for i in range(0, 100)]
        chunks = cloudwatch.segment_metric_data(metric_list)
        total_metrics = 0
        for chunk in chunks:
            assert len(chunk) <= cloudwatch.MAX_METRIC_DATA
            total_metrics += len(chunk)
        assert total_metrics == 100

    def test_segment_metrics_segmentation_not_required(self):
        cloudwatch = CloudWatchClient()
        metric_list = [i for i in range(0, 10)]
        chunks = cloudwatch.segment_metric_data(metric_list)
        total_metrics = 0
        for chunk in chunks:
            assert len(chunk) <= cloudwatch.MAX_METRIC_DATA
            total_metrics += len(chunk)
        assert total_metrics == 10
