from unittest import TestCase
import os
import boto3
import uuid
import zipfile
import json
import time

LOG_LEVEL = "LOG_LEVEL"
UNSUPPORTED_FILE_HANDLING = "UNSUPPORTED_FILE_HANDLING"
MAX_DOC_SIZE_PII_CLASSIFICATION = "MAX_DOC_SIZE_PII_CLASSIFICATION"
MAX_DOC_SIZE_PII_DETECTION = "MAX_DOC_SIZE_PII_DETECTION"
PII_ENTITY_TYPES = "PII_ENTITY_TYPES"
MASK_CHARACTER = "MASK_CHARACTER"
MASK_MODE = "MASK_MODE"
SUBSEGMENT_OVERLAPPING_TOKENS = "SUBSEGMENT_OVERLAPPING_TOKENS"
DEFAULT_MAX_DOC_SIZE = "DEFAULT_MAX_DOC_SIZE"
CONFIDENCE_THRESHOLD = "CONFIDENCE_THRESHOLD"
MAX_CHARS_OVERLAP = "MAX_CHARS_OVERLAP"
DEFAULT_LANGUAGE_CODE = "DEFAULT_LANGUAGE_CODE"
DETECT_PII_ENTITIES_THREAD_COUNT = "DETECT_PII_ENTITIES_THREAD_COUNT"
CLASSIFY_PII_DOC_THREAD_COUNT = "CLASSIFY_PII_DOC_THREAD_COUNT"
REDACTION_API_ONLY = "REDACTION_API_ONLY"
PUBLISH_CLOUD_WATCH_METRICS = "PUBLISH_CLOUD_WATCH_METRICS"


class BasicIntegTest(TestCase):

    REGION_NAME = 'us-east-1'
    BUILD_DIR = '.aws-sam/build/PiiAccessControlFunction/'
    REDACTION_HANDLER = "handler.redact_pii_documents_handler"
    ACCESS_CONTROL_HANDLER = "handler.pii_access_control_handler"

    @classmethod
    def _zip_function_code(cls):
        source_dir = cls.BUILD_DIR
        output_filename = "banner_function.zip"
        relroot = os.path.abspath(source_dir)
        with zipfile.ZipFile(output_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(source_dir):
                # add directory (needed for empty dirs)
                zipf.write(root, os.path.relpath(root, relroot))
                for file in files:
                    filename = os.path.join(root, file)
                    if os.path.isfile(filename): # regular files only
                        arcname = os.path.join(os.path.relpath(root, relroot), file)
                        zipf.write(filename, arcname)

        return output_filename

    @classmethod
    def _create_function(cls):
        cls.lambda_client = boto3.client('lambda', region_name=cls.REGION_NAME)

        cls.function_name = f"banner-integ-test-lambda-{cls.test_id}"
        zip_file_name = cls._zip_function_code()
        with open(zip_file_name, 'rb') as file_data:
            bytes_content = file_data.read()

        cls._create_role()

        create_function_response = cls.lambda_client.create_function(
            FunctionName=cls.function_name,
            Runtime='python3.8',
            Role=cls.role_arn,
            Handler='handler.redact_pii_documents_handler',
            Code={
                'ZipFile': bytes_content
            },
            Timeout=60,
            MemorySize=128,
            Publish=True,
            Environment={
                'Variables': {
                    LOG_LEVEL: 'DEBUG',
                    UNSUPPORTED_FILE_HANDLING: 'PASS',
                    MAX_DOC_SIZE_PII_CLASSIFICATION: '5000',
                    MAX_DOC_SIZE_PII_DETECTION: '5000',
                    PII_ENTITY_TYPES: 'ALL',
                    MASK_CHARACTER: '*',
                    MASK_MODE: 'MASK',
                    SUBSEGMENT_OVERLAPPING_TOKENS: '20',
                    DEFAULT_MAX_DOC_SIZE: '1048576',
                    CONFIDENCE_THRESHOLD: '0.5',
                    MAX_CHARS_OVERLAP: '200',
                    DEFAULT_LANGUAGE_CODE: 'en',
                    DETECT_PII_ENTITIES_THREAD_COUNT: '20',
                    CLASSIFY_PII_DOC_THREAD_COUNT: '20',
                    REDACTION_API_ONLY: 'True',
                    PUBLISH_CLOUD_WATCH_METRICS: 'True'
                }
            }
        )
        cls.function_arn = create_function_response['FunctionArn']

    @classmethod
    def _update_function_handler(cls, handler):
        response = cls.lambda_client.update_function_configuration(
            FunctionName=cls.function_name,
            Handler=handler
        )

    @classmethod
    def _create_role(cls):

        cls.role_name = f"banner-integ-test-role-{cls.test_id}"

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
            Path='/banner-integ-test/',
            RoleName=cls.role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy_document)
        )

        cls.role_arn = create_iam_role_response['Role']['Arn']

        policy_document = {
            "Statement": [
                {
                    "Action": [
                        "comprehend:DetectPiiEntities",
                        "comprehend:ClassifyPiiDocument"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "ComprehendPiiDetectionPolicy"
                },
                {
                    "Action": [
                        "s3-banner:WriteGetObjectResponse"
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
            PolicyName='BannerFunctionPolicy',
            PolicyDocument=json.dumps(policy_document)
        )

        attach_role_policy_response = cls.iam_client.attach_role_policy(
            RoleName=cls.role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )

    @classmethod
    def _create_bucket(cls):
        cls.bucket_name = f"banner-integ-test-{cls.test_id}"
        bucket = cls.s3.Bucket(cls.bucket_name)
        create_bucket_response = bucket.create()

    @classmethod
    def _create_access_point(cls):
        cls.access_point_name = f"banner-integ-test-ac-{cls.test_id}"

        create_access_point_response = cls.s3_ctrl.create_access_point(
            AccountId=cls.account_id,
            Name=cls.access_point_name,
            Bucket=cls.bucket_name,
        )
        cls.access_point_arn = f"arn:aws:s3:{cls.REGION_NAME}:{cls.account_id}:accesspoint/{cls.access_point_name}"

    @classmethod
    def _create_banner_access_point(cls):
        cls.banner_access_point_name = f"banner-integ-test-bac-{cls.test_id}"

        create_banner_access_point_response = cls.s3_ctrl.create_banner_access_point(
            AccountId=cls.account_id,
            Name=cls.banner_access_point_name,
            Configuration={
                'DefaultAccessPoint': cls.access_point_arn,
                'TransformationConfigurations': [
                    {
                        'Actions': ['s3:GetObject'],
                        'ContentTransformation': {
                            'AwsLambda': {
                                'FunctionArn': cls.function_arn,
                                'FunctionPayload':  '{}'
                            }
                        }
                    }
                ]
            }
        )
        cls.banner_access_point_arn = f"arn:aws:s3-banner:{cls.REGION_NAME}:{cls.account_id}:accesspoint/{cls.banner_access_point_name}"

    @classmethod
    def _upload_data(cls):
        cls.test_pii_obj_name = "pii_input.txt"
        test_pii_object_s3 = cls.s3.Object(cls.bucket_name, cls.test_pii_obj_name)
        with open(f"test/data/{cls.test_pii_obj_name}") as test_file:
            test_data = test_file.read()
        test_pii_obj_response = test_pii_object_s3.put(Body=test_data)

        cls.test_clean_obj_name = "clean.txt"
        test_clean_object_s3 = cls.s3.Object(cls.bucket_name, cls.test_clean_obj_name)
        with open(f"test/data/{cls.test_clean_obj_name}") as test_file:
            test_data = test_file.read()
        test_clean_obj_response = test_clean_object_s3.put(Body=test_data)

    @classmethod
    def setUpClass(cls):
        cls.s3 = boto3.resource('s3', region_name=cls.REGION_NAME)
        cls.s3_ctrl = boto3.client('s3control', region_name=cls.REGION_NAME)
        cls.iam = boto3.resource('iam')
        cls.account_id = cls.iam.CurrentUser().arn.split(':')[4]
        cls.iam_client = boto3.client('iam')
        cls.test_id = str(uuid.uuid4())[0:8]

        cls._create_function()
        cls._create_bucket()
        cls._create_access_point()
        cls._create_banner_access_point()
        cls._upload_data()

    @classmethod
    def tearDownClass(cls):
        delete_banner_access_point_response = cls.s3_ctrl.delete_banner_access_point(
            AccountId=cls.account_id,
            Name=cls.banner_access_point_name
        )
        delete_access_point_response = cls.s3_ctrl.delete_access_point(
            AccountId=cls.account_id,
            Name=cls.access_point_name
        )
        bucket = cls.s3.Bucket(cls.bucket_name)
        delete_object_response = bucket.objects.delete()

        delete_bucket_response = bucket.delete()

        delete_function_response = cls.lambda_client.delete_function(
            FunctionName=cls.function_name
        )

        delete_role_policy_response = cls.iam_client.delete_role_policy(
            RoleName=cls.role_name,
            PolicyName='BannerFunctionPolicy'
        )

        detach_role_policy_response = cls.iam_client.detach_role_policy(
            RoleName=cls.role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )

        delete_role_response = cls.iam_client.delete_role(
            RoleName=cls.role_name
        )

    def test_redaction_handler_with_pii(self):
        self._update_function_handler(self.REDACTION_HANDLER)

        test_pii_obj = self.s3.Object(self.banner_access_point_arn, self.test_pii_obj_name)
        test_obj_response = test_pii_obj.get()
        test_obj_data = test_obj_response['Body'].read().decode('utf-8')

        with open("test/data/pii_output.txt") as expected_output_file:
            expected_output = expected_output_file.read()

        assert expected_output == test_obj_data

    def test_redaction_handler_no_pii(self):
        self._update_function_handler(self.REDACTION_HANDLER)

        test_clean_obj = self.s3.Object(self.banner_access_point_arn, self.test_clean_obj_name)
        test_obj_response = test_clean_obj.get()
        test_obj_data = test_obj_response['Body'].read().decode('utf-8')

        with open(f"test/data/{self.test_clean_obj_name}") as expected_output_file:
            expected_output = expected_output_file.read()

        assert expected_output == test_obj_data

    def test_classification_handler_with_pii(self):
        self._update_function_handler(self.ACCESS_CONTROL_HANDLER)

        test_pii_obj = self.s3.Object(self.banner_access_point_arn, self.test_pii_obj_name)
        self.assertRaises(Exception, "test_pii_obj.get()")

    def test_classification_handler_no_pii(self):
        self._update_function_handler(self.ACCESS_CONTROL_HANDLER)

        test_clean_obj = self.s3.Object(self.banner_access_point_arn, self.test_clean_obj_name)
        test_obj_response = test_clean_obj.get()
        test_obj_data = test_obj_response['Body'].read().decode('utf-8')

        with open(f"test/data/{self.test_clean_obj_name}") as expected_output_file:
            expected_output = expected_output_file.read()

        assert expected_output == test_obj_data