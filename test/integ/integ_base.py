import logging
from datetime import datetime, timedelta
from time import sleep
from unittest import TestCase
import os
import boto3
import uuid
import zipfile
import json
import time

from dateutil.tz import tzutc

LOG_LEVEL = "LOG_LEVEL"
UNSUPPORTED_FILE_HANDLING = "UNSUPPORTED_FILE_HANDLING"
DOCUMENT_MAX_SIZE_CONTAINS_PII_ENTITIES = "DOCUMENT_MAX_SIZE_CONTAINS_PII_ENTITIES"
DOCUMENT_MAX_SIZE_DETECT_PII_ENTITIES = "DOCUMENT_MAX_SIZE_DETECT_PII_ENTITIES"
PII_ENTITY_TYPES = "PII_ENTITY_TYPES"
MASK_CHARACTER = "MASK_CHARACTER"
MASK_MODE = "MASK_MODE"
SUBSEGMENT_OVERLAPPING_TOKENS = "SUBSEGMENT_OVERLAPPING_TOKENS"
DOCUMENT_MAX_SIZE = "DOCUMENT_MAX_SIZE"
CONFIDENCE_THRESHOLD = "CONFIDENCE_THRESHOLD"
MAX_CHARS_OVERLAP = "MAX_CHARS_OVERLAP"
DEFAULT_LANGUAGE_CODE = "DEFAULT_LANGUAGE_CODE"
DETECT_PII_ENTITIES_THREAD_COUNT = "DETECT_PII_ENTITIES_THREAD_COUNT"
CONTAINS_PII_ENTITIES_THREAD_COUNT = "CONTAINS_PII_ENTITIES_THREAD_COUNT"
REDACTION_API_ONLY = "REDACTION_API_ONLY"
PUBLISH_CLOUD_WATCH_METRICS = "PUBLISH_CLOUD_WATCH_METRICS"
CW_METRIC_PUBLISH_CHECK_ATTEMPT = 5


class BasicIntegTest(TestCase):
    REGION_NAME = 'us-east-1'
    DATA_PATH = "test/data/integ"
    PII_ENTITY_TYPES_IN_TEST_DOC = ['EMAIL', 'PHONE', 'BANK_ROUTING', 'BANK_ACCOUNT_NUMBER', 'ADDRESS', 'DATE_TIME']

    @classmethod
    def _zip_function_code(cls, build_dir):
        source_dir = build_dir
        output_filename = "s3ol_function.zip"
        relroot = os.path.abspath(source_dir)
        with zipfile.ZipFile(output_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
            logging.info(f"Build Dir: {source_dir}")
            for root, dirs, files in os.walk(source_dir):
                # add directory (needed for empty dirs)
                zipf.write(root, os.path.relpath(root, relroot))
                for file in files:
                    filename = os.path.join(root, file)
                    if os.path.isfile(filename):  # regular files only
                        arcname = os.path.join(os.path.relpath(root, relroot), file)
                        zipf.write(filename, arcname)

        return output_filename

    @classmethod
    def _update_lambda_env_variables(cls, function_arn, env_variable_dict):
        function_name = function_arn.split(':')[-1]
        cls.lambda_client.update_function_configuration(FunctionName=function_name, Environment={'Variables': env_variable_dict})
        waiter = cls.lambda_client.get_waiter('function_updated')
        waiter.wait(FunctionName=function_name)

    @classmethod
    def _create_function(cls, function_name, handler_name, test_id, env_vars_dict, lambda_execution_role, build_dir):
        function_unique_name = f"{function_name}-{test_id}"
        zip_file_name = cls._zip_function_code(build_dir)
        with open(zip_file_name, 'rb') as file_data:
            bytes_content = file_data.read()

        create_function_response = cls.lambda_client.create_function(
            FunctionName=function_unique_name,
            Runtime='python3.8',
            Role=lambda_execution_role,
            Handler=handler_name,
            Code={
                'ZipFile': bytes_content
            },
            Timeout=60,
            MemorySize=128,
            Publish=True,
            Environment={
                'Variables': env_vars_dict
            }
        )
        return create_function_response['FunctionArn']

    @classmethod
    def _update_function_handler(cls, handler):
        response = cls.lambda_client.update_function_configuration(
            FunctionName=cls.function_name,
            Handler=handler
        )

    @classmethod
    def _create_role(cls, test_id):

        cls.role_name = f"s3ol-integ-test-role-{test_id}"

        assume_role_policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "lambda.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }

        create_iam_role_response = cls.iam_client.create_role(
            Path='/s3ol-integ-test/',
            RoleName=cls.role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy_document)
        )

        role_arn = create_iam_role_response['Role']['Arn']

        policy_document = {
            "Statement": [
                {
                    "Action": [
                        "comprehend:DetectPiiEntities",
                        "comprehend:ContainsPiiEntities"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "ComprehendPiiDetectionPolicy"
                },
                {
                    "Action": [
                        "s3-object-lambda:WriteGetObjectResponse"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "S3AccessPointCallbackPolicy"
                },
                {
                    "Action": [
                        "cloudwatch:PutMetricData"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "CloudWatchMetricsPolicy"
                }
            ]
        }

        put_role_policy_response = cls.iam_client.put_role_policy(
            RoleName=cls.role_name,
            PolicyName='S3OLFunctionPolicy',
            PolicyDocument=json.dumps(policy_document)
        )

        attach_role_policy_response = cls.iam_client.attach_role_policy(
            RoleName=cls.role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )
        # wait for some time to let iam role propagate
        sleep(10)
        return role_arn

    @classmethod
    def _create_bucket(cls, test_id):
        cls.bucket_name = f"s3ol-integ-test-{test_id}"
        bucket = cls.s3.Bucket(cls.bucket_name)
        create_bucket_response = bucket.create()
        cls.s3.BucketVersioning(cls.bucket_name).enable()

    @classmethod
    def _create_access_point(cls, test_id):
        cls.access_point_name = f"s3ol-integ-test-ac-{test_id}"

        create_access_point_response = cls.s3_ctrl.create_access_point(
            AccountId=cls.account_id,
            Name=cls.access_point_name,
            Bucket=cls.bucket_name,
        )
        cls.access_point_arn = f"arn:aws:s3:{cls.REGION_NAME}:{cls.account_id}:accesspoint/{cls.access_point_name}"

    @classmethod
    def _create_s3ol_access_point(cls, test_id, lambda_function_arn):
        cls.s3ol_access_point_name = f"s3ol-integ-test-bac-{test_id}"

        create_s3ol_access_point_response = cls.s3_ctrl.create_access_point_for_object_lambda(
            AccountId=cls.account_id,
            Name=cls.s3ol_access_point_name,
            Configuration={
                'SupportingAccessPoint': cls.access_point_arn,
                'TransformationConfigurations': [
                    {
                        'Actions': ['GetObject'],
                        'ContentTransformation': {
                            'AwsLambda': {
                                'FunctionArn': lambda_function_arn
                            }
                        }
                    }
                ],
                "AllowedFeatures": ["GetObject-Range"]
            }
        )
        return f"arn:aws:s3-object-lambda:{cls.REGION_NAME}:{cls.account_id}:accesspoint/{cls.s3ol_access_point_name}"

    @classmethod
    def _create_temporary_data_files(cls):
        non_pii_text = "\nentities identified in the input text. For each entity, the response provides the entity type, where the entity text begins and ends, and the level of confidence entities identified in the input text. For each entity, the response provides the entity type, where the entity text begins and ends, and the level of confidence that Amazon Comprehend has in the detectio"

        def _create_file(input_file_name, output_file_name, repeats=4077):
            with open(f"{cls.DATA_PATH}/{input_file_name}") as existing_file:
                modified_file_content = existing_file.read()
                for i in range(0, repeats):
                    modified_file_content += non_pii_text
            with open(f"{cls.DATA_PATH}/{output_file_name}", 'w') as new_file:
                new_file.seek(0)
                new_file.write(modified_file_content)

        _create_file('pii_input.txt', '1mb_pii_text')
        _create_file('pii_input.txt', '5mb_pii_text', repeats=15000)
        _create_file('pii_output.txt', '1mb_pii_redacted_text')

    @classmethod
    def _clear_temporary_files(cls):
        os.remove(f"{cls.DATA_PATH}/1mb_pii_text")
        os.remove(f"{cls.DATA_PATH}/5mb_pii_text")
        os.remove(f"{cls.DATA_PATH}/1mb_pii_redacted_text")

    @classmethod
    def _upload_data(cls):
        for filename in os.listdir(cls.DATA_PATH):
            cls.s3_client.upload_file(f"{cls.DATA_PATH}/{filename}", cls.bucket_name, filename)

    @classmethod
    def setUpClass(cls):
        cls.s3 = boto3.resource('s3', region_name=cls.REGION_NAME)
        cls.s3_client = boto3.client('s3', region_name=cls.REGION_NAME)
        cls.s3_ctrl = boto3.client('s3control', region_name=cls.REGION_NAME)
        cls.lambda_client = boto3.client('lambda', region_name=cls.REGION_NAME)
        cls.cloudwatch_client = boto3.client('cloudwatch', region_name=cls.REGION_NAME)
        cls.iam = boto3.resource('iam')
        cls.account_id = cls.iam.CurrentUser().arn.split(':')[4]
        cls.iam_client = boto3.client('iam')
        test_run_id = str(uuid.uuid4())[0:8]

        cls.lambda_role_arn = cls._create_role(test_run_id)
        cls._create_bucket(test_run_id)
        cls._create_access_point(test_run_id)
        cls._create_temporary_data_files()
        cls._upload_data()

    @classmethod
    def tearDownClass(cls):
        try:
            delete_access_point_response = cls.s3_ctrl.delete_access_point(
                AccountId=cls.account_id,
                Name=cls.access_point_name
            )
        except Exception as e:
            logging.error(e)
        cls._clear_temporary_files()
        bucket = cls.s3.Bucket(cls.bucket_name)
        delete_object_response = bucket.object_versions.all().delete()
        delete_bucket_response = bucket.delete()

        delete_role_policy_response = cls.iam_client.delete_role_policy(
            RoleName=cls.role_name,
            PolicyName='S3OLFunctionPolicy'
        )

        detach_role_policy_response = cls.iam_client.detach_role_policy(
            RoleName=cls.role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )

        delete_role_response = cls.iam_client.delete_role(
            RoleName=cls.role_name
        )

    def _validate_pii_count_metric_published(self, s3ol_access_point_arn, start_time: datetime, entity_types):
        end_time = start_time + timedelta(minutes=1)
        st_time = start_time - timedelta(minutes=1)

        def _is_metric_published() -> bool:
            for e_type in entity_types:
                pii_doc_processed_count_metric = \
                    self.cloudwatch_client.get_metric_data(MetricDataQueries=[{'Id': 'a1232454353',
                                                                               'MetricStat': {'Metric': {
                                                                                   'MetricName': 'PiiDocumentTypesProcessed',
                                                                                   'Namespace': 'ComprehendS3ObjectLambda',
                                                                                   'Dimensions': [
                                                                                       {'Name': 'PiiEntityType',
                                                                                        'Value': e_type},
                                                                                       {'Name': 'Language',
                                                                                        'Value': 'en'},
                                                                                       {'Name': 'S3ObjectLambdaAccessPoint',
                                                                                        'Value': s3ol_access_point_arn}]},
                                                                                   'Period': 60,
                                                                                   'Stat': 'Sum'}}],
                                                           StartTime=st_time,
                                                           EndTime=end_time)
                for result in pii_doc_processed_count_metric['MetricDataResults']:
                    if result['Id'] == "a1232454353":
                        if len(result['Values']) == 0:
                            return False
            return True

        attempts = CW_METRIC_PUBLISH_CHECK_ATTEMPT
        while attempts > 0:
            if _is_metric_published():
                return None
            sleep(10)
            attempts -= 1
        assert False, f"No metrics published for s3ol arn {s3ol_access_point_arn} for one of the entity types: {entity_types}," \
                      f"StartTime: {st_time}, EndTime: {end_time}"

    def _validate_api_call_latency_published(self, s3ol_access_point_arn, start_time: datetime, end_time=None,
                                             is_pii_classification_performed: bool = True,
                                             is_pii_detection_performed: bool = False,
                                             is_s3ol_callback_done: bool = True):
        end_time = start_time + timedelta(minutes=1) if not end_time else end_time
        st_time = start_time - timedelta(minutes=1)

        def _is_metric_published(api_name, service, ) -> bool:
            pii_doc_processed_count_metric = \
                self.cloudwatch_client.get_metric_data(MetricDataQueries=[{'Id': 'a1232454353',
                                                                           'MetricStat': {'Metric': {
                                                                               'MetricName': 'Latency',
                                                                               'Namespace': 'ComprehendS3ObjectLambda',
                                                                               'Dimensions': [
                                                                                   {'Name': 'API',
                                                                                    'Value': api_name},
                                                                                   {'Name': 'S3ObjectLambdaAccessPoint',
                                                                                    'Value': s3ol_access_point_arn},
                                                                                   {'Name': 'Service',
                                                                                    'Value': service}
                                                                               ]},
                                                                               'Period': 60,
                                                                               'Stat': 'Average'}}],
                                                       StartTime=st_time,
                                                       EndTime=end_time)
            for result in pii_doc_processed_count_metric['MetricDataResults']:
                if result['Id'] == "a1232454353":
                    if len(result['Values']) == 0:
                        return False
            return True

        attempts = CW_METRIC_PUBLISH_CHECK_ATTEMPT
        metric_checked = {
            'ContainsPiiEntities': False,
            'DetectPiiEntities': False,
            'WriteGetObjectResponse': False
        }
        while attempts > 0:
            if is_pii_classification_performed and not metric_checked['ContainsPiiEntities']:
                if _is_metric_published(api_name="ContainsPiiEntities", service="Comprehend"):
                    metric_checked['ContainsPiiEntities'] = True
            if is_pii_detection_performed and not metric_checked['DetectPiiEntities']:
                if _is_metric_published(api_name="DetectPiiEntities", service="Comprehend"):
                    metric_checked['DetectPiiEntities'] = True
            if is_s3ol_callback_done and not metric_checked['WriteGetObjectResponse']:
                if _is_metric_published(api_name="WriteGetObjectResponse", service="S3"):
                    metric_checked['WriteGetObjectResponse'] = True
            sleep(10)
            attempts -= 1
        if is_pii_classification_performed:
            assert metric_checked['ContainsPiiEntities'], \
                f"Could not verify that metrics were published for various API call made between StartTime: {st_time}, EndTime: {end_time}"
        if is_pii_detection_performed:
            assert metric_checked['DetectPiiEntities'], \
                f"Could not verify that metrics were published for various API call made between StartTime: {st_time}, EndTime: {end_time}"
        if is_s3ol_callback_done:
            assert metric_checked['WriteGetObjectResponse'], \
                f"Could not verify that metrics were published for API call made between StartTime: {st_time}, EndTime: {end_time}"
