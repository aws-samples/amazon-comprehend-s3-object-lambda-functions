import uuid
from datetime import datetime, timedelta

import boto3
import botocore
from botocore.exceptions import ClientError
from dateutil.tz import tzutc

from integ.integ_base import BasicIntegTest


class PiiAccessControlIntegTest(BasicIntegTest):
    BUILD_DIR = '.aws-sam/build/PiiAccessControlFunction/'
    PII_ENTITY_TYPES_IN_TEST_DOC = ['EMAIL', 'ADDRESS', 'NAME', 'PHONE', 'DATE_TIME']

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        test_run_id = str(uuid.uuid4())[0:8]
        cls.lambda_function_arn = cls._create_function("pii_access_control", 'handler.pii_access_control_handler',
                                                       test_run_id, {"LOG_LEVEL": "DEBUG"}, cls.lambda_role_arn, cls.BUILD_DIR)
        cls.s3ol_access_point_arn = cls._create_s3ol_access_point(test_run_id, cls.lambda_function_arn)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.s3_ctrl.delete_access_point_for_object_lambda(
            AccountId=cls.account_id,
            Name=cls.s3ol_access_point_arn.split('/')[-1]
        )

        cls.lambda_client.delete_function(
            FunctionName=cls.lambda_function_arn.split(':')[-1]
        )
        super().tearDownClass()

    def tearDown(self) -> None:
        self._update_lambda_env_variables(self.lambda_function_arn, {"LOG_LEVEL": "DEBUG"})

    def test_classification_lambda_with_default_entity_types(self):
        start_time = datetime.now(tz=tzutc())
        test_pii_obj = self.s3.Object(self.s3ol_access_point_arn, 'pii_input.txt')
        with self.assertRaises(ClientError) as e:
            test_pii_obj.get()
        assert e.exception.response['Error']['Message'] == "Document Contains PII"
        assert e.exception.response['Error']['Code'] == "AccessDenied"
        self._validate_pii_count_metric_published(self.s3ol_access_point_arn, start_time, self.PII_ENTITY_TYPES_IN_TEST_DOC)
        self._validate_api_call_latency_published(self.s3ol_access_point_arn, start_time)

    def test_classification_lambda_with_pii_overridden_entity_types(self):
        self._update_lambda_env_variables(self.lambda_function_arn,
                                          {"LOG_LEVEL": "DEBUG", "PII_ENTITY_TYPES": "USERNAME,PASSWORD,AWS_ACCESS_KEY"})
        response = self.s3_client.get_object(Bucket=self.s3ol_access_point_arn, Key='pii_input.txt')
        with open(f"{self.DATA_PATH}/pii_input.txt") as expected_output_file:
            assert response['Body'].read().decode('utf-8') == expected_output_file.read()

    def test_classification_lambda_throws_invalid_request_error_when_file_size_exceeds_limit(self):
        self._update_lambda_env_variables(self.lambda_function_arn, {"LOG_LEVEL": "DEBUG", "DOCUMENT_MAX_SIZE": "500"})
        test_pii_obj = self.s3.Object(self.s3ol_access_point_arn, 'pii_input.txt')
        with self.assertRaises(ClientError) as e:
            test_pii_obj.get()
        assert e.exception.response['Error']['Message'] == "Size of the requested object exceeds maximum file size supported"
        assert e.exception.response['Error']['Code'] == "EntityTooLarge"

    def test_classification_lambda_throws_access_denied_with_an_overridden_max_file_size_limit(self):
        self._update_lambda_env_variables(self.lambda_function_arn, {"LOG_LEVEL": "DEBUG", "DOCUMENT_MAX_SIZE": "1500000",
                                                                     "DOCUMENT_MAX_SIZE_CONTAINS_PII_ENTITIES": "5000"})
        with self.assertRaises(ClientError) as e:
            self.s3_client.get_object(Bucket=self.s3ol_access_point_arn, Key='1mb_pii_text')
        assert e.exception.response['Error']['Message'] == "Document Contains PII"
        assert e.exception.response['Error']['Code'] == "AccessDenied"

    def test_classification_lambda_with_unsupported_file(self):
        test_obj = self.s3.Object(self.s3ol_access_point_arn, 'RandomImage.png')
        with self.assertRaises(ClientError) as e:
            test_obj.get()
        assert e.exception.response['Error']['Message'] == "Unsupported file encountered for determining Pii"
        assert e.exception.response['Error']['Code'] == "UnexpectedContent"

    def test_classification_lambda_with_unsupported_file_handling_set_to_pass(self):
        self._update_lambda_env_variables(self.lambda_function_arn, {"LOG_LEVEL": "DEBUG", "UNSUPPORTED_FILE_HANDLING": 'PASS'})
        response = self.s3_client.get_object(Bucket=self.s3ol_access_point_arn, Key='RandomImage.png')
        assert 'Body' in response

    def test_classification_lambda_with_partial_object(self):
        with self.assertRaises(ClientError) as e:
            self.s3_client.get_object(Bucket=self.s3ol_access_point_arn, Key="pii_input.txt", Range="bytes=0-100")
        assert e.exception.response['Error']['Message'] == "HTTP Header Range is not supported"

    def test_classification_lambda_with_partial_object_allowed_with_versioned(self):
        file_name = 'pii_output.txt'
        self.s3_client.upload_file(f"{self.DATA_PATH}/{file_name}", self.bucket_name, file_name)
        versions = set()
        self._update_lambda_env_variables(self.lambda_function_arn, {"LOG_LEVEL": "DEBUG", "IS_PARTIAL_OBJECT_SUPPORTED": "TRUE"})

        for version in self.s3_client.list_object_versions(Bucket=self.bucket_name)['Versions']:
            if version['Key'] == 'pii_output.txt':
                versions.add(version['VersionId'])
        assert len(versions) >= 2, f"Expected at least 2 different versions of {file_name}"
        for versionId in versions:
            response = self.s3_client.get_object(Bucket=self.s3ol_access_point_arn, Key=file_name, Range="bytes=0-100",
                                                 VersionId=versionId)
            assert response['ContentRange'] == "bytes 0-100/611"
            assert response['ContentLength'] == 101
            assert response['ContentType'] == 'binary/octet-stream'
            assert response['VersionId'] == versionId

    def test_request_timeout(self):
        self._update_lambda_env_variables(self.lambda_function_arn,
                                          {"LOG_LEVEL": "DEBUG", "DOCUMENT_MAX_SIZE": "10000000",
                                           "CONTAINS_PII_ENTITIES_THREAD_COUNT": "1",
                                           "DOCUMENT_MAX_SIZE_CONTAINS_PII_ENTITIES": "5000"})
        start_time = datetime.now(tz=tzutc())
        with self.assertRaises(ClientError) as e:
            session_config = botocore.config.Config(
                retries={
                    'max_attempts': 0
                })
            s3_client = boto3.client('s3', region_name=self.REGION_NAME, config=session_config)
            response = s3_client.get_object(Bucket=self.s3ol_access_point_arn, Key='5mb_pii_text')
        end_time = datetime.now(tz=tzutc()) + timedelta(minutes=1)

        self._validate_api_call_latency_published(self.s3ol_access_point_arn, start_time, end_time)
        assert e.exception.response['Error']['Message'] == "Failed to complete document processing within time limit"
        assert e.exception.response['Error']['Code'] == "RequestTimeout"


    def test_classification_handler_no_pii(self):
        non_pii_file_name = 'clean.txt'
        start_time = datetime.now(tz=tzutc())
        test_clean_obj = self.s3.Object(self.s3ol_access_point_arn, non_pii_file_name)
        get_obj_response = test_clean_obj.get()
        get_obj_data = get_obj_response['Body'].read().decode('utf-8')

        with open(f"{self.DATA_PATH}/{non_pii_file_name}") as expected_output_file:
            expected_output = expected_output_file.read()

        assert expected_output == get_obj_data
        self._validate_api_call_latency_published(self.s3ol_access_point_arn, start_time)
