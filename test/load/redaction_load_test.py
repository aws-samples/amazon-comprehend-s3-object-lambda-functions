import logging
import time
import uuid

from load.load_test_base import BaseLoadTest


class PiiRedactionLoadTest(BaseLoadTest):
    BUILD_DIR = '.aws-sam/build/PiiRedactionFunction/'
    PII_ENTITY_TYPES_IN_TEST_DOC = ['EMAIL', 'ADDRESS', 'NAME', 'PHONE', 'DATE_TIME']

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        test_run_id = str(uuid.uuid4())[0:8]
        cls.lambda_function_arn = cls._create_function("pii_redaction_load_test", 'handler.redact_pii_documents_handler',
                                                       test_run_id, {"LOG_LEVEL": "DEBUG"}, cls.lambda_role_arn, cls.BUILD_DIR)
        cls.s3ol_access_point_arn = cls._create_s3ol_access_point(test_run_id, cls.lambda_function_arn)
        cls._update_lambda_env_variables(cls.lambda_function_arn, {"LOG_LEVEL": "DEBUG",
                                                                   "DOCUMENT_MAX_SIZE": "1600000"})
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

    def test_redaction_lambda_with_varying_load(self):
        variations = [(1, True),
                      (5, False),
                      (5, True),
                      (50, False),
                      (50, True),
                      (1000, False),
                      (1000, True),
                      (1500, False),
                      (1500, True)
                      ]
        for text_size, is_pii in variations:
            self.execute_load_on_get_object(self.s3ol_access_point_arn, text_size, is_pii, None)
            time.sleep(60)

    def test_redaction_lambda_find_max_conn(self):
        variations = [
                      (1, True),
                      (5, False),
                      (5, True),
                      (50, False),
                      (50, True),
                      (1000, False),
                      (1000, True),
                      (1500, False),
                      (1500, True)
                      ]
        for text_size, is_pii in variations:
            self.find_max_tpm(self.s3ol_access_point_arn, text_size, is_pii, None)
