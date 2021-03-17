import logging
import time
import uuid

from botocore.exceptions import ClientError

from load.load_test_base import BaseLoadTest


class PiiAccessControlLoadTest(BaseLoadTest):
    BUILD_DIR = '.aws-sam/build/PiiAccessControlFunction/'
    PII_ENTITY_TYPES_IN_TEST_DOC = ['EMAIL', 'ADDRESS', 'NAME', 'PHONE', 'DATE_TIME']

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        test_run_id = str(uuid.uuid4())[0:8]
        cls.lambda_function_arn = cls._create_function("pii_access_control_load_test", 'handler.pii_access_control_handler',
                                                       test_run_id, {"LOG_LEVEL": "DEBUG"}, cls.lambda_role_arn, cls.BUILD_DIR)
        cls.s3ol_access_point_arn = cls._create_s3ol_access_point(test_run_id, cls.lambda_function_arn)
        cls._update_lambda_env_variables(cls.lambda_function_arn, {"LOG_LEVEL": "DEBUG",
                                                                   "DOCUMENT_MAX_SIZE": "1500000"})
        logging.info(f"Created access point: {cls.s3ol_access_point_arn}  for testing")

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

    def test_access_control_lambda_with_varying_load(self):
        document_sizes = [1, 5, 50, 1000, 1500]
        for size in document_sizes:
            self.find_max_tpm(self.s3ol_access_point_arn,size, True, ClientError)
            time.sleep(90)
