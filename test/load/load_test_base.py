import logging
import os
import time
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

from integ.integ_base import BasicIntegTest


class BaseLoadTest(BasicIntegTest):
    DEFAULT_PERIOD = 5
    DEFAULT_CONNECTIONS = 1

    def find_max_tpm(self, s3_olap_arn, file_size, is_pii, expected_error, error_percent_threshold=0.05, load_test_period=3):
        file_name = self.create_and_upload_text_file(file_size, is_pii)
        total_counts, failed_counts = self.run_desired_simulataneous_get_object_calls(s3_olap_arn, file_name, expected_error,
                                                                                      period_in_minutes=load_test_period)
        current_tpm = total_counts / load_test_period
        previous_tpm = 0
        previous_best_connection_count = self.DEFAULT_CONNECTIONS
        connection_count = self.DEFAULT_CONNECTIONS
        while failed_counts / total_counts < error_percent_threshold and previous_tpm <= current_tpm:
            previous_tpm = current_tpm
            previous_best_connection_count = connection_count
            connection_count = previous_best_connection_count + 2
            total_counts, failed_counts = self.run_desired_simulataneous_get_object_calls(s3_olap_arn, file_name, expected_error,
                                                                                          connection_counts=connection_count,
                                                                                          period_in_minutes=load_test_period)
            current_tpm = total_counts / load_test_period

        logging.info(
            f"Best results tpm: {previous_tpm} with error rate :{failed_counts / total_counts * 100} %  for file size {file_size} "
            f"are obtained with Connection count as {previous_best_connection_count}")

    def execute_load_on_get_object(self, s3_olap_arn, file_size, is_pii, expected_error):
        logging.info(f"Running Load Test for {file_size} KB file where pii is {'' if is_pii else 'not'} present")
        file_name = self.create_and_upload_text_file(file_size, is_pii)
        return self.run_desired_simulataneous_get_object_calls(s3_olap_arn, file_name, expected_error)

    def create_and_upload_text_file(self, desired_size_in_KB: int, is_pii: bool):
        file_name = str("pii_" if is_pii else "non_pii") + str(desired_size_in_KB) + "_KB"

        repeat_text = " Some Random Text ttt"
        with open(self.DATA_PATH + "/pii_input.txt") as pii_file:
            pii_text = pii_file.read()
        full_text = pii_text if is_pii else ""
        with open(file_name, 'w') as temp:
            while len(full_text) <= desired_size_in_KB * 1000:
                full_text += repeat_text
            temp.write(full_text)
        self.s3_client.upload_file(file_name, self.bucket_name, file_name)
        os.remove(file_name)
        return file_name

    def run_desired_simulataneous_get_object_calls(self, s3_olap_arn, file_name, expected_error, connection_counts=DEFAULT_CONNECTIONS,
                                                   period_in_minutes=DEFAULT_PERIOD, ):
        logging.info(
            f"Running Load Test for file : {file_name}  for period {period_in_minutes} with connection counts: {connection_counts}")
        s = ThreadPoolExecutor(max_workers=connection_counts)
        futures = [s.submit(self.fail_safe_fetch_s3_object, s3_olap_arn, file_name, period=period_in_minutes * 60,
                            expected_error=expected_error) for i in
                   range(0, connection_counts)]

        total_counts = 0
        successful_counts = 0
        average_latency = 0
        for f in as_completed(futures):
            successful_counts += f.result()[1]
            total_counts += f.result()[0] + f.result()[1]
            average_latency = f.result()[2] / total_counts

        logging.info(f" Total calls made: {total_counts}, out of which {successful_counts} calls were successful."
                     f" ({successful_counts / total_counts * 100}%) ,Average Latency {average_latency}")
        return total_counts, total_counts - successful_counts, average_latency

    def fail_safe_fetch_s3_object(self, s3ol_ap_arn, file, period, expected_error=None):
        start_time = time.time()
        failed_calls = 0
        successful_calls = 0
        total_time = 0
        while (time.time() - start_time) <= period:
            try:
                api_call_st_time = time.time()
                response = self.s3_client.get_object(Bucket=s3ol_ap_arn, Key=file)
                total_time += time.time() - api_call_st_time
                successful_calls += 1
            except Exception as e:
                total_time += time.time() - api_call_st_time
                if expected_error:
                    if isinstance(e, expected_error):
                        successful_calls += 1
                    else:
                        failed_calls += 1
                else:
                    # logging.error(e)
                    failed_calls += 1
        return failed_calls, successful_calls, total_time
