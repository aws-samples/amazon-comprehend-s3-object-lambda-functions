import time
from time import sleep
from unittest import TestCase

from exceptions import TimeoutException, FileSizeLimitExceededException
from util import execute_task_with_timeout


class UtilTest(TestCase):

    def test_execute_task_with_timeout_with_time_limit_exceeded(self):
        with self.assertRaises(TimeoutException) as e:
            start_time = time.time()

            def sleep_2():
                sleep(5)

            execute_task_with_timeout(1000, sleep_2)
        elapsed_time = time.time() - start_time
        assert 1000 <= elapsed_time * 1000 <= 1100

    def test_execute_task_with_timeout_time_limit_not_exceeded(self):
        start_time = time.time()

        def sleep_1():
            sleep(1)

        execute_task_with_timeout(2000, sleep_1)
        elapsed_time = time.time() - start_time
        assert 1000 <= elapsed_time * 1000 <= 1100

    def test_execute_task_with_timeout_when_task_fails(self):
        def task():
            raise FileSizeLimitExceededException()

        with self.assertRaises(FileSizeLimitExceededException) as e:
            execute_task_with_timeout(2000, task)
