"""Utility Class."""
import concurrent
from concurrent.futures._base import TimeoutError

import lambdalogging
from exceptions import TimeoutException

LOG = lambdalogging.getLogger(__name__)


def execute_task_with_timeout(timeout_in_millis, task):
    """
    Execute a given task within a given time limit.
    :param timeout_in_millis: milliseconds to timeout
    :param task: task to execute
    :raise: TimeoutException
    """
    timeout_in_sec = int(timeout_in_millis / 1000)
    try:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future_result = executor.submit(task)
        return future_result.result(timeout=timeout_in_sec)
    except TimeoutError:
        # Free up the resources
        executor.shutdown(wait=False)
        raise TimeoutException()
