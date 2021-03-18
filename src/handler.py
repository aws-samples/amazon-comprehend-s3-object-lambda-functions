"""Lambda function handler."""

# must be the first import in files with lambda function handlers
import time
import traceback

import lambdainit  # noqa: F401
import json
from typing import List
import lambdalogging
from clients.comprehend_client import ComprehendClient
from clients.s3_client import S3Client
from clients.cloudwatch_client import CloudWatchClient
from config import DOCUMENT_MAX_SIZE_CONTAINS_PII_ENTITIES, DOCUMENT_MAX_SIZE_DETECT_PII_ENTITIES, DEFAULT_LANGUAGE_CODE, \
    PUBLISH_CLOUD_WATCH_METRICS, REDACTION_API_ONLY, COMPREHEND_ENDPOINT_URL
from constants import ALL, REQUEST_ID, GET_OBJECT_CONTEXT, S3OL_ACCESS_POINT_ARN, \
    INPUT_S3_URL, S3OL_CONFIGURATION, REQUEST_ROUTE, REQUEST_TOKEN, PAYLOAD, DEFAULT_USER_AGENT, LANGUAGE_CODE, USER_REQUEST, \
    HEADERS, CONTENT_LENGTH, RESERVED_TIME_FOR_CLEANUP
from data_object import Document, PiiConfig, RedactionConfig, ClassificationConfig
from exception_handlers import ExceptionHandler
from exceptions import RestrictedDocumentException
from processors import Segmenter, Redactor
from util import execute_task_with_timeout
from validators import InputEventValidator, PartialObjectRequestValidator

LOG = lambdalogging.getLogger(__name__)


def get_interested_pii(document: Document, classification_config: PiiConfig):
    """
    Get a list of interested pii from the document.

    Return a list of pii entity types of the given document with only the entities of interest
    and above the confidence threshold.
    """
    pii_entities = []
    for name, score in document.pii_classification.items():
        if name in classification_config.pii_entity_types or ALL in classification_config.pii_entity_types:
            if score >= classification_config.confidence_threshold:
                pii_entities.append(name)
    return pii_entities


def publish_metrics(cloud_watch: CloudWatchClient, s3: S3Client, comprehend: ComprehendClient, processed_document: bool,
                    processed_pii_document: bool, language_code: str, s3ol_access_point: str, pii_entities: List[str]):
    """Publish metrics from the function execution."""
    try:
        cloud_watch.publish_metrics(s3.download_metrics.metrics + s3.write_get_object_metrics.metrics +
                                    comprehend.classify_metrics.metrics + comprehend.detection_metrics.metrics)
        if processed_document:
            cloud_watch.put_document_processed_metric(language_code, s3ol_access_point)
            if processed_pii_document:
                cloud_watch.put_pii_document_processed_metric(language_code, s3ol_access_point)
                cloud_watch.put_pii_document_types_metric(pii_entities, language_code, s3ol_access_point)
    except Exception as e:
        LOG.error(f"Error publishing metrics to cloudwatch. :{e} {traceback.print_exc()}")


def redact(text, classification_segmenter: Segmenter, detection_segmenter: Segmenter,
           redactor: Redactor, comprehend: ComprehendClient, redaction_config: RedactionConfig, language_code) -> Document:
    """
    Redact pii data from given text. Logic for redacting:- .

    1. Segment text into subsegments of reasonable sizes (max doc size supported by comprehend) for doing initial classification
    2. For each subsegment ,
        2.1 call comprehend's classify-pii-document api to determine if it contains any PII data
        2.2 if it contains pii then split it to smaller chunks(e.g. <=5KB), else skip to the next subsegment
        2.3 for each chunk
             2.3.1 call comprehend's detect-pii-entities to extract the pii entities
             2.3.2 redact the pii entities from the chunk
        2.4 merge all chunks
    3. merge all subsegments
    """
    if REDACTION_API_ONLY:
        doc = Document(text)
        documents = [doc]
        docs_for_entity_detection = detection_segmenter.segment(doc.text, doc.char_offset)
    else:
        documents = comprehend.contains_pii_entities(classification_segmenter.segment(text), language_code)
        pii_docs = [doc for doc in documents if len(get_interested_pii(doc, redaction_config)) > 0]
        if not pii_docs:
            LOG.debug("Document doesn't have any pii. Nothing to redact.")
            text = classification_segmenter.de_segment(documents).text
            return Document(text, redacted_text=text)
        docs_for_entity_detection = []
        for pii_doc in pii_docs:
            docs_for_entity_detection.extend(detection_segmenter.segment(pii_doc.text, pii_doc.char_offset))

    docs_with_pii_entities = comprehend.detect_pii_documents(docs_for_entity_detection, language_code)
    resultant_doc = classification_segmenter.de_segment(documents + docs_with_pii_entities)
    assert len(resultant_doc.text) == len(text), "Not able to recover original document after segmentation and desegmentation."
    redacted_text = redactor.redact(text, resultant_doc.pii_entities)
    resultant_doc.redacted_text = redacted_text
    return resultant_doc


def classify(text, classification_segmenter: Segmenter, comprehend: ComprehendClient,
             detection_config: ClassificationConfig, language_code) -> List[str]:
    """
    Detect pii data from given text. Logic for detecting:- .

    1. Segment text into segments of reasonable sizes (max doc size supported by comprehend) for
       doing initial classification
    2. For each segment,
        2.1 call comprehend's classify-pii-document api to determine if it contains any PII data
        2.2 if it contains pii that is in the detection config then return those pii, else move to the next segment
    3. If no pii detected, return empty list, else list of pii types found that is also in the detection config
       and above the given threshold
    """
    pii_classified_documents = comprehend.contains_pii_entities(classification_segmenter.segment(text), language_code)
    pii_types = set()
    for doc in pii_classified_documents:
        doc_pii_types = get_interested_pii(doc, detection_config)
        pii_types |= set(doc_pii_types)
    return list(pii_types)


def redact_pii_documents_handler(event, context):
    """Redaction Lambda function handler."""
    LOG.info('Received event with requestId: %s', event[REQUEST_ID])
    LOG.debug(f'Raw event {event}')

    InputEventValidator.validate(event)
    invoke_args = json.loads(event[S3OL_CONFIGURATION][PAYLOAD]) if event[S3OL_CONFIGURATION][PAYLOAD] else {}
    language_code = invoke_args.get(LANGUAGE_CODE, DEFAULT_LANGUAGE_CODE)
    redaction_config = RedactionConfig(**invoke_args)
    object_get_context = event[GET_OBJECT_CONTEXT]
    s3ol_access_point = event[S3OL_CONFIGURATION][S3OL_ACCESS_POINT_ARN]
    s3 = S3Client(s3ol_access_point)
    cloud_watch = CloudWatchClient()
    comprehend = ComprehendClient(s3ol_access_point=s3ol_access_point, session_id=event[REQUEST_ID], user_agent=DEFAULT_USER_AGENT,
                                  endpoint_url=COMPREHEND_ENDPOINT_URL)

    exception_handler = ExceptionHandler(s3)

    LOG.debug("Pii Entity Types to be redacted:" + str(redaction_config.pii_entity_types))
    processed_document = False
    document = Document('')

    try:
        def time_bound_task():
            nonlocal processed_document
            nonlocal document
            PartialObjectRequestValidator.validate(event)
            pii_classification_segmenter = Segmenter(DOCUMENT_MAX_SIZE_CONTAINS_PII_ENTITIES)
            pii_redaction_segmenter = Segmenter(DOCUMENT_MAX_SIZE_DETECT_PII_ENTITIES)
            redactor = Redactor(redaction_config)
            time1 = time.time()
            text, http_headers, status_code = s3.download_file_from_presigned_url(object_get_context[INPUT_S3_URL],
                                                                                  event[USER_REQUEST][HEADERS])
            time2 = time.time()
            LOG.info(f"Downloaded the file in : {(time2 - time1)} seconds")
            document = redact(text, pii_classification_segmenter, pii_redaction_segmenter, redactor,
                              comprehend, redaction_config, language_code)
            processed_document = True
            time1 = time.time()
            LOG.info(f"Pii redaction completed within {(time1 - time2)} seconds. Returning back the response to S3")
            redacted_text_bytes = document.redacted_text.encode('utf-8')
            http_headers[CONTENT_LENGTH] = len(redacted_text_bytes)
            s3.respond_back_with_data(redacted_text_bytes, http_headers, object_get_context[REQUEST_ROUTE],
                                      object_get_context[REQUEST_TOKEN], status_code)

        execute_task_with_timeout(context.get_remaining_time_in_millis() - RESERVED_TIME_FOR_CLEANUP, time_bound_task)
    except Exception as generated_exception:
        exception_handler.handle_exception(generated_exception, object_get_context[REQUEST_ROUTE], object_get_context[REQUEST_TOKEN])
    finally:
        if PUBLISH_CLOUD_WATCH_METRICS:
            pii_entities = get_interested_pii(document, redaction_config)
            publish_metrics(cloud_watch, s3, comprehend, processed_document, len(pii_entities) > 0, language_code,
                            s3ol_access_point, pii_entities)

    LOG.info("Responded back to s3 successfully")


def pii_access_control_handler(event, context):
    """Detect Lambda function handler."""
    LOG.info(f'Received event with requestId: {event[REQUEST_ID]}')
    LOG.debug(f'Raw event {event}')

    InputEventValidator.validate(event)
    invoke_args = json.loads(event[S3OL_CONFIGURATION][PAYLOAD]) if event[S3OL_CONFIGURATION][PAYLOAD] else {}
    language_code = invoke_args.get(LANGUAGE_CODE, DEFAULT_LANGUAGE_CODE)
    detection_config = ClassificationConfig(**invoke_args)
    object_get_context = event[GET_OBJECT_CONTEXT]
    s3ol_access_point = event[S3OL_CONFIGURATION][S3OL_ACCESS_POINT_ARN]

    s3 = S3Client(s3ol_access_point)
    cloud_watch = CloudWatchClient()
    comprehend = ComprehendClient(session_id=event[REQUEST_ID], user_agent=DEFAULT_USER_AGENT, endpoint_url=COMPREHEND_ENDPOINT_URL,
                                  s3ol_access_point=s3ol_access_point)
    exception_handler = ExceptionHandler(s3)

    LOG.debug("Pii Entity Types to be detected:" + str(detection_config.pii_entity_types))

    pii_classification_segmenter = Segmenter(DOCUMENT_MAX_SIZE_CONTAINS_PII_ENTITIES)

    processed_document = False
    processed_pii_document = False
    pii_entities = []

    try:
        def time_bound_task():
            nonlocal processed_document
            nonlocal processed_pii_document
            nonlocal pii_entities
            PartialObjectRequestValidator.validate(event)
            time1 = time.time()
            text, http_headers, status_code = s3.download_file_from_presigned_url(object_get_context[INPUT_S3_URL],
                                                                                  event[USER_REQUEST][HEADERS])
            time2 = time.time()
            LOG.info(f"Downloaded the file in : {(time2 - time1)} seconds")
            pii_entities = classify(text, pii_classification_segmenter, comprehend, detection_config, language_code)
            time1 = time.time()

            processed_document = True
            LOG.info(f"Pii detection completed within {(time1 - time2)} seconds. Returning back the response to S3")
            if len(pii_entities) > 0:
                processed_pii_document = True
                raise RestrictedDocumentException()
            else:
                text_bytes = text.encode('utf-8')
                http_headers[CONTENT_LENGTH] = len(text_bytes)
                s3.respond_back_with_data(text_bytes, http_headers, object_get_context[REQUEST_ROUTE],
                                          object_get_context[REQUEST_TOKEN],
                                          status_code)

        execute_task_with_timeout(context.get_remaining_time_in_millis() - RESERVED_TIME_FOR_CLEANUP, time_bound_task)
    except Exception as generated_exception:
        exception_handler.handle_exception(generated_exception, object_get_context[REQUEST_ROUTE], object_get_context[REQUEST_TOKEN])
    finally:
        if PUBLISH_CLOUD_WATCH_METRICS:
            publish_metrics(cloud_watch, s3, comprehend, processed_document, processed_pii_document, language_code,
                            s3ol_access_point, pii_entities)

    LOG.info("Responded back to s3 successfully")
