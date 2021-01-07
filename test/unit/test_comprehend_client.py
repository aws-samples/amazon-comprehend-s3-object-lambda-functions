from time import sleep, time
from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from botocore.awsrequest import AWSRequest

from clients.comprehend_client import ComprehendClient
from constants import BEGIN_OFFSET, END_OFFSET, ENTITY_TYPE, SCORE
from data_object import Document


class ComprehendClientTest(TestCase):
    @patch('clients.comprehend_client.boto3')
    def test_comprehend_client_constuctor(self, mocked_boto3):
        mocked_client = MagicMock()
        mocked_boto3.client.return_value = mocked_client
        comprehend_client = ComprehendClient()
        mocked_client.meta.events.register.assert_called_with('before-sign.comprehend.*', comprehend_client._add_session_header)
        request = AWSRequest()
        comprehend_client._add_session_header(request)
        assert len(request.headers.get('x-amzn-session-id')) >= 10
        assert comprehend_client.classification_executor_service._max_workers == 2
        assert comprehend_client.redaction_executor_service._max_workers == 5

    @patch('clients.comprehend_client.boto3')
    def test_comprehend_detect_pii_entities(self, mocked_boto3):
        DUMMY_PII_ENTITY = {BEGIN_OFFSET: 12, END_OFFSET: 14, ENTITY_TYPE: 'SSN', SCORE: 0.345}

        def mocked_api_call(**kwargs):
            sleep(0.1)
            return {'Entities': [DUMMY_PII_ENTITY]}

        mocked_client = MagicMock()
        mocked_boto3.client.return_value = mocked_client
        comprehend_client = ComprehendClient()
        mocked_client.detect_pii_entities.side_effect = mocked_api_call
        start_time = time()
        docs_with_pii_entity = comprehend_client.detect_pii_documents(documents=[Document(text="Some Random 1mb_text", ) for i in range(1, 20)],
                                                                      language='en')
        end_time = time()
        mocked_client.detect_pii_entities.assert_has_calls([call(Text="Some Random 1mb_text", LanguageCode='en') for i in range(1, 20)])
        # should be around 0.4 : 20 calls with 5 thread counts , where each call taking 0.1 seconds to complete
        assert 0.4 <= end_time - start_time < 0.5
        for doc in docs_with_pii_entity:
            assert len(doc.pii_entities) == 1
            assert doc.pii_entities[0] == DUMMY_PII_ENTITY

    @patch('clients.comprehend_client.boto3')
    def test_comprehend_classify_pii_document(self, mocked_boto3):
        classification_result = {'Labels': [{'Name': 'SSN', 'Score': 0.1234}]}

        def mocked_api_call(**kwargs):
            sleep(0.1)
            return classification_result

        mocked_client = MagicMock()
        mocked_boto3.client.return_value = mocked_client
        comprehend_client = ComprehendClient()
        mocked_client.classify_pii_document.side_effect = mocked_api_call
        start_time = time()
        docs_with_pii_classification = comprehend_client.classify_pii_documents(
            documents=[Document(text="Some Random 1mb_text", ) for i in range(1, 4)],
            language='en')
        end_time = time()

        mocked_client.classify_pii_document.assert_has_calls([call(Text="Some Random 1mb_text", LanguageCode='en') for i in range(1, 4)])
        # should be around 0.2 : 4 calls with 2 thread counts , where each call taking 0.1 seconds to complete
        assert 0.2 <= end_time - start_time < 0.3
        for doc in docs_with_pii_classification:
            assert doc.pii_classification == {'SSN': 0.1234}

    @patch('clients.comprehend_client.boto3')
    def test_comprehend_classify_pii_document_failure(self, mocked_boto3):
        classification_result = {'Labels': [{'Name': 'SSN', 'Score': 0.1234}]}

        mocked_client = MagicMock()
        mocked_boto3.client.return_value = mocked_client
        comprehend_client = ComprehendClient()
        api_invocation_exception = Exception("Some unrecoverable error")
        mocked_client.classify_pii_document.side_effect = [classification_result, classification_result, api_invocation_exception,
                                                           classification_result]
        try:
            comprehend_client.classify_pii_documents(documents=[Document(text="Some Random 1mb_text", ) for i in range(1, 4)],
                                                     language='en')

            assert False, "Expected an exception "
        except Exception as e:
            assert e == api_invocation_exception
        mocked_client.classify_pii_document.assert_has_calls([call(Text="Some Random 1mb_text", LanguageCode='en') for i in range(1, 4)])

    @patch('clients.comprehend_client.boto3')
    def test_comprehend_detect_pii_entities_failure(self, mocked_boto3):
        DUMMY_PII_ENTITY = {'Entities': [{BEGIN_OFFSET: 12, END_OFFSET: 14, ENTITY_TYPE: 'SSN', SCORE: 0.345}]}
        mocked_client = MagicMock()
        mocked_boto3.client.return_value = mocked_client
        comprehend_client = ComprehendClient()
        api_invocation_exception = Exception("Some unrecoverable error")
        mocked_client.detect_pii_entities.side_effect = [DUMMY_PII_ENTITY, DUMMY_PII_ENTITY, api_invocation_exception,
                                                         DUMMY_PII_ENTITY]
        try:
            comprehend_client.detect_pii_documents(documents=[Document(text="Some Random 1mb_text", ) for i in range(1, 4)],
                                                   language='en')

            assert False, "Expected an exception "
        except Exception as e:
            assert e == api_invocation_exception
        mocked_client.detect_pii_entities.assert_has_calls([call(Text="Some Random 1mb_text", LanguageCode='en') for i in range(1, 4)])
