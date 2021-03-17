import json
import os
from copy import deepcopy
from time import sleep
from unittest import TestCase
from unittest.mock import MagicMock, patch

from config import DEFAULT_LANGUAGE_CODE, DOCUMENT_MAX_SIZE_CONTAINS_PII_ENTITIES, DOCUMENT_MAX_SIZE_DETECT_PII_ENTITIES
from constants import INPUT_S3_URL, GET_OBJECT_CONTEXT, REQUEST_ROUTE, REQUEST_TOKEN, S3_STATUS_CODES, S3_ERROR_CODES, USER_REQUEST, \
    HEADERS, CONTENT_LENGTH
from data_object import Document, RedactionConfig, ClassificationConfig
from exceptions import UnsupportedFileException, FileSizeLimitExceededException
from handler import get_interested_pii, redact, redact_pii_documents_handler, classify, pii_access_control_handler
from processors import Segmenter, Redactor

this_module_path = os.path.dirname(__file__)


class HandlersTest(TestCase):
    def setUp(self) -> None:
        self.mocked_context = MagicMock()
        self.mocked_context.get_remaining_time_in_millis.return_value = 60000

    def test_get_interested_pii_true(self):
        assert len(get_interested_pii(Document(text="Some Random text", pii_classification={'SSN': 0.534}),
                                      RedactionConfig())) > 0
        assert len(get_interested_pii(Document(text="Some Random text", pii_classification={'SSN': 0.734}),
                                      RedactionConfig(pii_entity_types=['SSN'], confidence_threshold=0.7))) > 0

    def test_get_interested_pii_false(self):
        assert len(get_interested_pii(Document(text="Some Random text"),
                                      RedactionConfig())) == 0
        assert len(get_interested_pii(Document(text="Some Random text", pii_classification={'SSN': 0.234}),
                                      RedactionConfig(pii_entity_types=['NAME']))) == 0
        assert len(get_interested_pii(Document(text="Some Random text", pii_classification={'SSN': 0.534}),
                                      RedactionConfig(pii_entity_types=['SSN'], confidence_threshold=0.7))) == 0

    def test_redact_with_pii_and_classification(self):
        comprehend_client = MagicMock()

        comprehend_client.contains_pii_entities.return_value = [Document(text="Some Random text", pii_classification={'SSN': 0.53})]
        comprehend_client.detect_pii_documents.return_value = [Document(text="Some Random text", pii_classification={'SSN': 0.53},
                                                                        pii_entities=[{'Score': 0.534, 'Type': 'SSN', 'BeginOffset': 0,
                                                                                       'EndOffset': 4}])]

        document = redact("Some Random text", Segmenter(DOCUMENT_MAX_SIZE_CONTAINS_PII_ENTITIES),
                          Segmenter(DOCUMENT_MAX_SIZE_DETECT_PII_ENTITIES),
                          Redactor(RedactionConfig()), comprehend_client, RedactionConfig(),
                          DEFAULT_LANGUAGE_CODE)
        comprehend_client.contains_pii_entities.assert_called_once()
        comprehend_client.detect_pii_documents.assert_called_once()
        assert document.redacted_text == "**** Random text"

    @patch('handler.REDACTION_API_ONLY', False)
    def test_redact_with_no_pii_and_classification(self):
        comprehend_client = MagicMock()

        comprehend_client.contains_pii_entities.return_value = [Document(text="Some Random text", pii_classification={})]
        document = redact("Some Random text", Segmenter(DOCUMENT_MAX_SIZE_CONTAINS_PII_ENTITIES),
                          Segmenter(DOCUMENT_MAX_SIZE_DETECT_PII_ENTITIES),
                          Redactor(RedactionConfig()), comprehend_client, RedactionConfig(),
                          DEFAULT_LANGUAGE_CODE)
        comprehend_client.contains_pii_entities.assert_called_once()
        comprehend_client.detect_pii_documents.assert_not_called()
        assert document.redacted_text == "Some Random text"

    @patch('handler.REDACTION_API_ONLY', True)
    def test_redact_with_pii_and_only_redaction(self):
        comprehend_client = MagicMock()

        comprehend_client.contains_pii_entities.return_value = [Document(text="Some Random text", pii_classification={'SSN': 0.53})]
        comprehend_client.detect_pii_documents.return_value = [Document(text="Some Random text", pii_classification={'SSN': 0.53},
                                                                        pii_entities=[{'Score': 0.534, 'Type': 'SSN', 'BeginOffset': 0,
                                                                                       'EndOffset': 4}])]

        document = redact("Some Random text", Segmenter(DOCUMENT_MAX_SIZE_CONTAINS_PII_ENTITIES),
                          Segmenter(DOCUMENT_MAX_SIZE_DETECT_PII_ENTITIES),
                          Redactor(RedactionConfig()), comprehend_client, RedactionConfig(),
                          DEFAULT_LANGUAGE_CODE)
        comprehend_client.contains_pii_entities.assert_not_called()
        comprehend_client.detect_pii_documents.assert_called_once()
        assert document.redacted_text == "**** Random text"

    @patch('handler.REDACTION_API_ONLY', True)
    def test_redact_with_no_pii_and_only_redaction(self):
        comprehend_client = MagicMock()

        comprehend_client.contains_pii_entities.return_value = [Document(text="Some Random text", pii_classification={})]
        comprehend_client.detect_pii_documents.return_value = [Document(text="Some Random text", pii_entities={})]
        document = redact("Some Random text", Segmenter(DOCUMENT_MAX_SIZE_CONTAINS_PII_ENTITIES),
                          Segmenter(DOCUMENT_MAX_SIZE_DETECT_PII_ENTITIES),
                          Redactor(RedactionConfig()), comprehend_client, RedactionConfig(),
                          DEFAULT_LANGUAGE_CODE)
        comprehend_client.contains_pii_entities.assert_not_called()
        comprehend_client.detect_pii_documents.assert_called_once()
        assert document.redacted_text == "Some Random text"

    def test_classify_with_no_pii(self):
        comprehend_client = MagicMock()

        comprehend_client.contains_pii_entities.return_value = [Document(text="Some Random text", pii_classification={})]
        entities = classify("Some Random text", Segmenter(DOCUMENT_MAX_SIZE_CONTAINS_PII_ENTITIES), comprehend_client,
                            ClassificationConfig(),
                            DEFAULT_LANGUAGE_CODE)
        comprehend_client.contains_pii_entities.assert_called_once()
        assert len(entities) == 0

    def test_classify_with_pii(self):
        comprehend_client = MagicMock()

        comprehend_client.contains_pii_entities.return_value = [
            Document(text="Some Random text", pii_classification={'SSN': 0.53, 'PHONE': 0.49, 'NAME': 0.99})
        ]
        entities = classify("Some Random text", Segmenter(DOCUMENT_MAX_SIZE_CONTAINS_PII_ENTITIES), comprehend_client,
                            ClassificationConfig(),
                            DEFAULT_LANGUAGE_CODE)
        comprehend_client.contains_pii_entities.assert_called_once()
        assert len(entities) == 2
        assert 'SSN' in entities
        assert 'NAME' in entities

    @patch('handler.CloudWatchClient')
    @patch('handler.redact')
    @patch('handler.S3Client')
    def test_redaction_handler_success(self, s3_client, mocked_redact, cloudwatch):
        with open(os.path.join(this_module_path, "..", 'data', 'sample_event.json'), 'r') as file_pointer:
            sample_event = json.load(file_pointer)

        sample_text = "Some Random text"
        sample_redacted_text = "Some **** text"
        mocked_s3_client = MagicMock()
        s3_client.return_value = mocked_s3_client
        s3_get_object_response_http_headers = {'response-header': 'value2'}
        expected_response_http_headers = deepcopy(s3_get_object_response_http_headers)
        expected_response_http_headers[CONTENT_LENGTH] = len(sample_redacted_text.encode('utf-8'))

        mocked_s3_client.download_file_from_presigned_url.return_value = sample_text, s3_get_object_response_http_headers, \
                                                                         S3_STATUS_CODES.PARTIAL_CONTENT_206
        mocked_redact.return_value = Document(sample_text, redacted_text=sample_redacted_text)

        mocked_cloudwatch = MagicMock()
        cloudwatch.return_value = mocked_cloudwatch

        redact_pii_documents_handler(sample_event, self.mocked_context)
        mocked_redact.assert_called_once()
        mocked_s3_client.download_file_from_presigned_url.assert_called_once_with(sample_event[GET_OBJECT_CONTEXT][INPUT_S3_URL],
                                                                                  sample_event[USER_REQUEST][HEADERS])
        mocked_s3_client.respond_back_with_data.assert_called_once_with(sample_redacted_text.encode('utf-8'),
                                                                        expected_response_http_headers,
                                                                        sample_event[GET_OBJECT_CONTEXT][REQUEST_ROUTE],
                                                                        sample_event[GET_OBJECT_CONTEXT][REQUEST_TOKEN],
                                                                        S3_STATUS_CODES.PARTIAL_CONTENT_206)

    @patch('handler.redact')
    @patch('handler.CloudWatchClient')
    @patch('handler.S3Client')
    def test_redaction_handler_success_lambda_timedout(self, s3_client, cloudwatch, mocked_redact):
        with open(os.path.join(this_module_path, "..", 'data', 'sample_event.json'), 'r') as file_pointer:
            sample_event = json.load(file_pointer)
        sample_text = "Some Random text"
        mocked_s3_client = MagicMock()
        s3_client.return_value = mocked_s3_client
        mocked_cloudwatch = MagicMock()
        self.mocked_context.get_remaining_time_in_millis.return_value = 3000
        cloudwatch.return_value = mocked_cloudwatch
        s3_get_object_response_http_headers = {'response-header': 'value2'}
        expected_response_http_headers = deepcopy(s3_get_object_response_http_headers)
        expected_response_http_headers[CONTENT_LENGTH] = len(sample_text.encode('utf-8'))

        mocked_s3_client.download_file_from_presigned_url.return_value = sample_text, s3_get_object_response_http_headers, \
                                                                         S3_STATUS_CODES.PARTIAL_CONTENT_206

        def _time_consuming_call(*args, **kwargs):
            sleep(5)
            return Document(sample_text, redacted_text=sample_text)

        mocked_redact.side_effect = _time_consuming_call
        redact_pii_documents_handler(sample_event, self.mocked_context)
        mocked_redact.assert_called_once()
        mocked_s3_client.download_file_from_presigned_url.assert_called_once_with(sample_event[GET_OBJECT_CONTEXT][INPUT_S3_URL],
                                                                                  sample_event[USER_REQUEST][HEADERS])
        mocked_s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.BAD_REQUEST_400,
                                                                         S3_ERROR_CODES.RequestTimeout,
                                                                         "Failed to complete document processing within time limit",
                                                                         sample_event[GET_OBJECT_CONTEXT][REQUEST_ROUTE],
                                                                         sample_event[GET_OBJECT_CONTEXT][REQUEST_TOKEN])

        mocked_cloudwatch.publish_metrics.assert_called_once()

    @patch('handler.CloudWatchClient')
    @patch('handler.redact')
    @patch('handler.S3Client')
    def test_redaction_handler_success_empty_payload(self, s3_client, mocked_redact, cloudwatch):
        with open(os.path.join(this_module_path, "..", 'data', 'sample_event.json'), 'r') as file_pointer:
            sample_event = json.load(file_pointer)
        sample_event['configuration']['payload'] = ""
        sample_text = "Some Random text"
        sample_redacted_text = "Some **** text"
        mocked_s3_client = MagicMock()
        s3_client.return_value = mocked_s3_client
        s3_get_object_response_http_headers = {'response-header': 'value2'}
        expected_response_http_headers = deepcopy(s3_get_object_response_http_headers)
        expected_response_http_headers[CONTENT_LENGTH] = len(sample_redacted_text.encode('utf-8'))

        mocked_s3_client.download_file_from_presigned_url.return_value = sample_text, s3_get_object_response_http_headers, \
                                                                         S3_STATUS_CODES.PARTIAL_CONTENT_206
        mocked_redact.return_value = Document(sample_text, redacted_text=sample_redacted_text)

        mocked_cloudwatch = MagicMock()
        cloudwatch.return_value = mocked_cloudwatch

        redact_pii_documents_handler(sample_event, self.mocked_context)
        mocked_redact.assert_called_once()
        mocked_s3_client.download_file_from_presigned_url.assert_called_once_with(sample_event[GET_OBJECT_CONTEXT][INPUT_S3_URL],
                                                                                  sample_event[USER_REQUEST][HEADERS])
        mocked_s3_client.respond_back_with_data.assert_called_once_with(sample_redacted_text.encode('utf-8'),
                                                                        expected_response_http_headers,
                                                                        sample_event[GET_OBJECT_CONTEXT][REQUEST_ROUTE],
                                                                        sample_event[GET_OBJECT_CONTEXT][REQUEST_TOKEN],
                                                                        S3_STATUS_CODES.PARTIAL_CONTENT_206)

    @patch('handler.CloudWatchClient')
    @patch('handler.redact')
    @patch('handler.S3Client')
    @patch('handler.ExceptionHandler')
    def test_redaction_handler_failure(self, exception_handler, s3_client, mocked_redact, cloudwatch):
        with open(os.path.join(this_module_path, "..", 'data', 'sample_event.json'), 'r') as file_pointer:
            sample_event = json.load(file_pointer)
        sample_redacted_text = "Some Random text"
        mocked_s3_client = MagicMock()
        mocked_exception_handler = MagicMock()
        exception_handler.return_value = mocked_exception_handler

        s3_client.return_value = mocked_s3_client
        exception = FileSizeLimitExceededException()
        mocked_s3_client.download_file_from_presigned_url.side_effect = [exception]
        mocked_redact.return_value = sample_redacted_text

        mocked_cloudwatch = MagicMock()
        cloudwatch.return_value = mocked_cloudwatch

        redact_pii_documents_handler(sample_event, self.mocked_context)
        mocked_s3_client.download_file_from_presigned_url.assert_called_once_with(sample_event[GET_OBJECT_CONTEXT][INPUT_S3_URL],
                                                                                  sample_event[USER_REQUEST][HEADERS])
        mocked_exception_handler.handle_exception.assert_called_once_with(exception,
                                                                          sample_event[GET_OBJECT_CONTEXT][REQUEST_ROUTE],
                                                                          sample_event[GET_OBJECT_CONTEXT][REQUEST_TOKEN])

    @patch('handler.classify')
    @patch('handler.CloudWatchClient')
    @patch('handler.S3Client')
    def test_detection_handler_success_no_pii(self, s3_client, cloudwatch, mocked_classify):
        with open(os.path.join(this_module_path, "..", 'data', 'sample_event.json'), 'r') as file_pointer:
            sample_event = json.load(file_pointer)
        sample_text = "Some Random text"
        mocked_s3_client = MagicMock()
        s3_client.return_value = mocked_s3_client
        mocked_cloudwatch = MagicMock()
        cloudwatch.return_value = mocked_cloudwatch
        s3_get_object_response_http_headers = {'response-header': 'value2'}
        expected_response_http_headers = deepcopy(s3_get_object_response_http_headers)
        expected_response_http_headers[CONTENT_LENGTH] = len(sample_text.encode('utf-8'))

        mocked_s3_client.download_file_from_presigned_url.return_value = sample_text, s3_get_object_response_http_headers, \
                                                                         S3_STATUS_CODES.PARTIAL_CONTENT_206
        mocked_classify.return_value = []

        pii_access_control_handler(sample_event, self.mocked_context)
        mocked_classify.assert_called_once()
        mocked_s3_client.download_file_from_presigned_url.assert_called_once_with(sample_event[GET_OBJECT_CONTEXT][INPUT_S3_URL],
                                                                                  sample_event[USER_REQUEST][HEADERS])
        mocked_s3_client.respond_back_with_data.assert_called_once_with(sample_text.encode('utf-8'), expected_response_http_headers,
                                                                        sample_event[GET_OBJECT_CONTEXT][REQUEST_ROUTE],
                                                                        sample_event[GET_OBJECT_CONTEXT][REQUEST_TOKEN],
                                                                        S3_STATUS_CODES.PARTIAL_CONTENT_206)

        mocked_cloudwatch.put_document_processed_metric.assert_called_once()

    @patch('handler.classify')
    @patch('handler.CloudWatchClient')
    @patch('handler.S3Client')
    def test_detection_handler_success_lambda_timedout(self, s3_client, cloudwatch, mocked_classify):
        with open(os.path.join(this_module_path, "..", 'data', 'sample_event.json'), 'r') as file_pointer:
            sample_event = json.load(file_pointer)
        sample_text = "Some Random text"
        mocked_s3_client = MagicMock()
        s3_client.return_value = mocked_s3_client
        mocked_cloudwatch = MagicMock()
        self.mocked_context.get_remaining_time_in_millis.return_value = 3000
        cloudwatch.return_value = mocked_cloudwatch
        s3_get_object_response_http_headers = {'response-header': 'value2'}
        expected_response_http_headers = deepcopy(s3_get_object_response_http_headers)
        expected_response_http_headers[CONTENT_LENGTH] = len(sample_text.encode('utf-8'))

        mocked_s3_client.download_file_from_presigned_url.return_value = sample_text, s3_get_object_response_http_headers, \
                                                                         S3_STATUS_CODES.PARTIAL_CONTENT_206

        def _time_consuming_call(*args, **kwargs):
            sleep(5)
            return []

        mocked_classify.side_effect = _time_consuming_call
        pii_access_control_handler(sample_event, self.mocked_context)
        mocked_classify.assert_called_once()
        mocked_s3_client.download_file_from_presigned_url.assert_called_once_with(sample_event[GET_OBJECT_CONTEXT][INPUT_S3_URL],
                                                                                  sample_event[USER_REQUEST][HEADERS])
        mocked_s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.BAD_REQUEST_400,
                                                                         S3_ERROR_CODES.RequestTimeout,
                                                                         "Failed to complete document processing within time limit",
                                                                         sample_event[GET_OBJECT_CONTEXT][REQUEST_ROUTE],
                                                                         sample_event[GET_OBJECT_CONTEXT][REQUEST_TOKEN])

        mocked_cloudwatch.publish_metrics.assert_called_once()

    @patch('handler.classify')
    @patch('handler.CloudWatchClient')
    @patch('handler.S3Client')
    def test_detection_handler_success_with_pii(self, s3_client, cloudwatch, mocked_classify):
        with open(os.path.join(this_module_path, "..", 'data', 'sample_event.json'), 'r') as file_pointer:
            sample_event = json.load(file_pointer)
        sample_text = "Some Random text with pii"
        mocked_s3_client = MagicMock()
        s3_client.return_value = mocked_s3_client
        mocked_cloudwatch = MagicMock()
        cloudwatch.return_value = mocked_cloudwatch
        s3_get_object_response_http_headers = {'response-header': 'value2'}
        mocked_s3_client.download_file_from_presigned_url.return_value = sample_text, s3_get_object_response_http_headers, \
                                                                         S3_STATUS_CODES.PARTIAL_CONTENT_206
        mocked_classify.return_value = ['SSN']

        pii_access_control_handler(sample_event, self.mocked_context)
        mocked_classify.assert_called_once()
        mocked_s3_client.download_file_from_presigned_url.assert_called_once_with(sample_event[GET_OBJECT_CONTEXT][INPUT_S3_URL],
                                                                                  sample_event[USER_REQUEST][HEADERS])
        mocked_s3_client.respond_back_with_error.assert_called_once_with(S3_STATUS_CODES.FORBIDDEN_403,
                                                                         S3_ERROR_CODES.AccessDenied,
                                                                         "Document Contains PII",
                                                                         sample_event[GET_OBJECT_CONTEXT][REQUEST_ROUTE],
                                                                         sample_event[GET_OBJECT_CONTEXT][REQUEST_TOKEN])

        mocked_cloudwatch.put_document_processed_metric.assert_called_once()

        mocked_cloudwatch.put_pii_document_processed_metric.assert_called_once()
        mocked_cloudwatch.put_pii_document_types_metric.assert_called_once()

    @patch('handler.classify')
    @patch('handler.CloudWatchClient')
    @patch('handler.S3Client')
    @patch('handler.ExceptionHandler')
    def test_detection_handler_failure(self, exception_handler, s3_client, cloudwatch, mocked_classify):
        with open(os.path.join(this_module_path, "..", 'data', 'sample_event.json'), 'r') as file_pointer:
            sample_event = json.load(file_pointer)
        mocked_s3_client = MagicMock()
        s3_client.return_value = mocked_s3_client
        mocked_cloudwatch = MagicMock()
        cloudwatch.return_value = mocked_cloudwatch

        mocked_exception_handler = MagicMock()
        exception_handler.return_value = mocked_exception_handler

        exception = UnsupportedFileException(file_content="File content", http_headers={'content-type': 'pdf'})
        mocked_s3_client.download_file_from_presigned_url.side_effect = [exception]
        mocked_classify.return_value = []

        pii_access_control_handler(sample_event, self.mocked_context)
        mocked_s3_client.download_file_from_presigned_url.assert_called_once_with(sample_event[GET_OBJECT_CONTEXT][INPUT_S3_URL],
                                                                                  sample_event[USER_REQUEST][HEADERS])
        mocked_exception_handler.handle_exception.assert_called_once_with(exception,
                                                                          sample_event[GET_OBJECT_CONTEXT][REQUEST_ROUTE],
                                                                          sample_event[GET_OBJECT_CONTEXT][REQUEST_TOKEN])
