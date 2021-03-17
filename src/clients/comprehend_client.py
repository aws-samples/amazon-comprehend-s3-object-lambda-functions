"""Client wrapper over aws services."""

import string
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from copy import deepcopy
from random import choices
from typing import List

import boto3
import botocore
import time

import lambdalogging
from clients.cloudwatch_client import Metrics
from config import CONTAINS_PII_ENTITIES_THREAD_COUNT, DETECT_PII_ENTITIES_THREAD_COUNT, DEFAULT_LANGUAGE_CODE
from constants import DEFAULT_USER_AGENT, CONTAINS_PII_ENTITIES, DETECT_PII_ENTITIES, COMPREHEND, COMPREHEND_MAX_RETRIES
from data_object import Document

LOG = lambdalogging.getLogger(__name__)


class ComprehendClient:
    """Wrapper over comprehend client."""

    def __init__(self, s3ol_access_point: str, pii_classification_thread_count: int = CONTAINS_PII_ENTITIES_THREAD_COUNT,
                 pii_redaction_thread_count: int = DETECT_PII_ENTITIES_THREAD_COUNT,
                 session_id: str = ''.join(choices(string.ascii_uppercase + string.digits, k=10)),
                 user_agent=DEFAULT_USER_AGENT, endpoint_url=None):
        self.session_id = session_id
        session_config = botocore.config.Config(
            user_agent_extra=user_agent,
            retries={
                'max_attempts': COMPREHEND_MAX_RETRIES,
                'mode': 'standard'
            })
        if endpoint_url is None:
            self.comprehend = boto3.client('comprehend', config=session_config)
        else:
            self.comprehend = boto3.client('comprehend', config=session_config, endpoint_url=endpoint_url, verify=False)
        self.comprehend.meta.events.register('before-sign.comprehend.*', self._add_session_header)
        self.classification_executor_service = ThreadPoolExecutor(max_workers=pii_classification_thread_count)
        self.redaction_executor_service = ThreadPoolExecutor(max_workers=pii_redaction_thread_count)
        self.classify_metrics = Metrics(service_name=COMPREHEND, api=CONTAINS_PII_ENTITIES, s3ol_access_point=s3ol_access_point)
        self.detection_metrics = Metrics(service_name=COMPREHEND, api=DETECT_PII_ENTITIES, s3ol_access_point=s3ol_access_point)

    def _add_session_header(self, request, **kwargs):
        request.headers.add_header('x-amzn-session-id', self.session_id)

    def contains_pii_entities(self, documents: List[Document], language=DEFAULT_LANGUAGE_CODE) -> List[Document]:
        """Call comprehend to get pii classification of given documents."""
        documents_copy = deepcopy(documents)
        result = []
        with self.classification_executor_service:
            futures = []
            for doc in documents_copy:
                futures.append(self.classification_executor_service.submit(self._update_doc_with_pii_classification, doc, language))

            for future_result in as_completed(futures):
                try:
                    result.append(future_result.result())
                except Exception as error:
                    LOG.error("Error occurred while calling comprehend for classifying text as pii", exc_info=True)
                    self.classify_metrics.add_fault_count()
                    raise error
        return result

    def _update_doc_with_pii_classification(self, document: Document, language) -> Document:
        start_time = time.time()
        response = None
        try:
            response = self.comprehend.contains_pii_entities(Text=document.text, LanguageCode=language)
        finally:
            if response is not None:
                self.classify_metrics.add_fault_count(response['ResponseMetadata']['RetryAttempts'])
            self.classify_metrics.add_latency(start_time, time.time())
        # updating the document itself instead of creating a new copy to save space
        document.pii_classification = {label['Name']: label['Score'] for label in response['Labels']}
        return document

    def detect_pii_documents(self, documents: List[Document], language=DEFAULT_LANGUAGE_CODE) -> List[Document]:
        """Call comprehend to get pii entities present in given documents."""
        documents_copy = deepcopy(documents)
        result = []
        with self.redaction_executor_service:
            futures = []
            for doc in documents_copy:
                futures.append(self.redaction_executor_service.submit(self._update_doc_with_pii_entities, doc, language))

            for future_result in as_completed(futures):
                try:
                    result.append(future_result.result())
                except Exception as error:
                    LOG.error("Error occurred while calling comprehend for detecting pii entities", exc_info=True)
                    self.detection_metrics.add_fault_count()
                    raise error
            return result

    def _update_doc_with_pii_entities(self, document: Document, language) -> Document:
        start_time = time.time()
        response = None
        try:
            response = self.comprehend.detect_pii_entities(Text=document.text, LanguageCode=language)
        finally:
            if response is not None:
                self.detection_metrics.add_fault_count(response['ResponseMetadata']['RetryAttempts'])
            self.detection_metrics.add_latency(start_time, time.time())
        # updating the document itself instead of creating a new copy to save space
        document.pii_entities = response['Entities']
        document.pii_classification = {entity['Type']: max(entity['Score'], document.pii_classification[entity['Type']])
                                       if entity['Type'] in document.pii_classification else entity['Score']
                                       for entity in response['Entities']}
        return document
