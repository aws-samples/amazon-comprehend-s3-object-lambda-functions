"""Client wrapper over aws services."""

from typing import List

import boto3

import lambdalogging
from constants import CLOUD_WATCH_NAMESPACE, LANGUAGE, COUNT, PII_DOCUMENTS_PROCESSED, DOCUMENTS_PROCESSED, NAME, \
    VALUE, BANNER_ACCESS_POINT, METRIC_NAME, UNIT, DIMENSIONS, PII_DOCUMENT_TYPES_PROCESSED, PII_ENTITY_TYPE, \
    LATENCY, API, SERVICE, FAULT_COUNT

LOG = lambdalogging.getLogger(__name__)


class Metrics:
    """Metrics class for latency and fault counts."""

    def __init__(self, service_name, api, cloudwatch_namespace=CLOUD_WATCH_NAMESPACE):
        self.cloudwatch_namespace = cloudwatch_namespace
        self.service_name = service_name
        self.api = api
        self.metrics = []

    def add_latency(self, start_time: int, end_time: int):
        """Add a latency metric."""
        self.metrics.append({METRIC_NAME: LATENCY, DIMENSIONS: [
            {NAME: API, VALUE: self.api},
            {NAME: SERVICE, VALUE: self.service_name}
        ], UNIT: COUNT, VALUE: (end_time - start_time) * 1000})

    def add_fault_count(self):
        """Add a fault count metric."""
        self.metrics.append({METRIC_NAME: FAULT_COUNT, DIMENSIONS: [
            {NAME: API, VALUE: self.api},
            {NAME: SERVICE, VALUE: self.service_name}
        ], UNIT: COUNT, VALUE: 1.0})


class CloudWatchClient:
    """Wrapper over cloudwatch client."""

    MAX_METRIC_DATA = 15

    def __init__(self):
        self.cloudwatch = boto3.client('cloudwatch')

    def segment_metric_data(self, metric_list: List):
        """Segments a list of arbitrary length into a list of lists each of size MAX_METRIC_DATA."""
        list_len = len(metric_list)
        return [metric_list[i: min((i + 1) * self.MAX_METRIC_DATA, list_len)]
                for i in range(int(list_len / self.MAX_METRIC_DATA) + min(list_len % self.MAX_METRIC_DATA, 1))]

    def publish_metrics(self, metric_list: List):
        """Publish the metrics to CloudWatch."""
        for metrics in self.segment_metric_data(metric_list):
            self.cloudwatch.put_metric_data(MetricData=metrics, Namespace=CLOUD_WATCH_NAMESPACE)

    def put_pii_document_processed_metric(self, language: str, banner_access_point: str):
        """Put PiiDocumentsProcessed metric."""
        self.publish_metrics([{METRIC_NAME: PII_DOCUMENTS_PROCESSED, DIMENSIONS: [
            {NAME: LANGUAGE, VALUE: language},
            {NAME: BANNER_ACCESS_POINT, VALUE: banner_access_point}
        ], UNIT: COUNT, VALUE: 1.0}])

    def put_document_processed_metric(self, language: str, banner_access_point: str):
        """Put DocumentsProcessed metric."""
        self.publish_metrics([{METRIC_NAME: DOCUMENTS_PROCESSED, DIMENSIONS: [
            {NAME: LANGUAGE, VALUE: language},
            {NAME: BANNER_ACCESS_POINT, VALUE: banner_access_point}
        ], UNIT: COUNT, VALUE: 1.0}])

    def put_pii_document_types_metric(self, pii_entity_types: List[str], language: str, banner_access_point: str):
        """Put PiiDocumentTypesProcessed metric."""
        self.publish_metrics([{METRIC_NAME: PII_DOCUMENT_TYPES_PROCESSED, DIMENSIONS: [
            {NAME: PII_ENTITY_TYPE, VALUE: pii_entity_type},
            {NAME: BANNER_ACCESS_POINT, VALUE: banner_access_point},
            {NAME: LANGUAGE, VALUE: language}
        ], UNIT: COUNT, VALUE: 1.0} for pii_entity_type in pii_entity_types])
