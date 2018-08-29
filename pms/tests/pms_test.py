import unittest
import sys
sys.path.append('../src')
import pms
import random

class TestPMSLib(unittest.TestCase):


    def test_insert_accuracy_report_success_with_existing_metric(self):
        report = {
            "metric_name":"quick_search",
            "submetrics": [],
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "ts":"2019-09-09 09:09:00",
            "report_message": "Report description for test No. 4"+str(random.randint(1,21)*5)
        }

        status = pms.insert_accuracy_report(report)
        self.assertTrue(status['status'])

    def test_insert_accuracy_report_success_with_existing_module(self):
        report = {
            "type": "accuracy",
            "module_name": "chatterbox",
            "metric_name":"quick_search",
            "submetrics": [],
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "ts":"2019-09-09 09:09:00",
            "report_message": "Report description for test No. 4"+str(random.randint(1,21)*5)
        }

        status = pms.insert_accuracy_report(report)
        self.assertTrue(status)

    def test_insert_accuracy_report_success_with_existing_team(self):
        report = {
            "type": "accuracy",
            "module_name": "chatterbox",
            "metric_name":"quick_search",
            "submetrics": [],
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "ts":"2019-09-09 09:09:00",
            "report_message": "Report description for test No. 4"+str(random.randint(1,21)*5)
        }
        status = pms.insert_accuracy_report(report)
        self.assertTrue(status)

    def test_insert_accuracy_report_success_with_utf8_characters(self):
        report = {
            "type": "accuracy",
            "module_name": "chatterbox",
            "metric_name":"quick_search",
            "submetrics": [],
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "ts":"2019-09-09 09:09:00",
            "report_message": "Report description for test No. 4"+str(random.randint(1,21)*5)
        }

        status = pms.insert_accuracy_report(report)
        self.assertTrue(status)

    def test_insert_accuracy_report_success_with_special_characters(self):
        report = {
            "type": "accuracy",
            "module_name": "chatterbox",
            "metric_name":"quick_search",
            "submetrics": [],
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "ts":"2019-09-09 09:09:00",
            "report_message": "Report description for test No. 4"+str(random.randint(1,21)*5)
        }

        status = pms.insert_accuracy_report(report)
        self.assertTrue(status)

    def test_insert_accuracy_report_success_with_sub_metrics(self):
        report = {
            "type": "accuracy",
            "module_name": "chatterbox",
            "metric_name":"quick_search",
            "ts":"2019-09-09 09:09:00",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": ['test1'],
            "report_message": "Report description for test No. 4"
        }

        status = pms.insert_accuracy_report(report)
        self.assertTrue(status)

    def test_insert_accuracy_report_fail_invalid_metric_name(self):
        report = {
            "type": "accuracy",
            "module_name": "chatterbox",
            "metric_name":"invalid_metric",
            "ts":"2019-09-09 09:09:00",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": [],
            "report_message": "Report description for test No. 4"
        }

        status = pms.insert_accuracy_report(report)
        self.assertFalse(status)

    def test_insert_accuracy_report_fail_invalid_date(self):
        report = {
            "type": "accuracy",
            "module_name": "chatterbox",
            "metric_name":"quick_search",
            "ts":"-0",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": [],
            "report_message": "Report description for test No. 4"
        }

        status = pms.insert_accuracy_report(report)
        self.assertFalse(status)

    def test_insert_accuracy_report_fail_invalid_all_fields(self):
        report = {
            "type": "accuracy",
            "module_name": "hg",
            "metric_name":"sdadsadsa",
            "ts":"09-09 :09:00",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": [],
            "report_message": "Report description for test No. 4"+str(random.randint(1,21)*9999999999999999)
        }

        status = pms.insert_accuracy_report(report)
        self.assertFalse(status)


# ------------------------------------------------------------------------------------------

    def test_insert_error_log_report_success_existing_module(self):
        report = {
            "type": "error_log",
            "metric_name":"quick_search",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": [],
            "ts":"2019-09-09 09:09:00",
            "report_message": "Report description for test No. 5"+str(random.randint(1,21)*25)
        }

        status = pms.insert_error_log_report(report)
        self.assertTrue(status)

    def test_insert_error_log_report_success_existing_metric(self):
        report = {
            "type": "error_log",
            "metric_name":"quick_search",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": [],
            "ts":"2019-09-09 09:09:00",
            "report_message": "Report description for test No. 5"+str(random.randint(1,21)*25)
        }

        status = pms.insert_error_log_report(report)
        self.assertTrue(status)

    def test_insert_error_log_report_success_with_utf8_character(self):
        report = {
            "type": "error_log",
            "metric_name":"quick_search",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": [],
            "ts":"2019-09-09 09:09:00",
            "report_message": "UTF 8 CHARACTER"+str(random.randint(1,21)*25)
        }

        status = pms.insert_error_log_report(report)
        self.assertTrue(status)

    def test_insert_error_log_report_success_with_sub_metrics(self):
        report = {
            "type": "error_log",
            "metric_name":"quick_search",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": ['test1'],
            "ts":"2019-09-09 09:09:00",
            "report_message": "Report description for test No. 5"
        }

        status = pms.insert_error_log_report(report)
        self.assertTrue(status)

    def test_insert_error_log_report_fail_with_invalid_metric_name(self):
        report = {
            "type": "error_log",
            "module_name": "chatterbox",
            "metric_name":"quick_seasdasdasdasdarch",
            "ts":"2019-09-09 09:09:00",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": [],
            "report_message": "Report description for test No. 5"
        }

        status = pms.insert_error_log_report(report)
        self.assertFalse(status)

    def test_insert_error_log_report_fail_with_invalid_timestamp(self):
        report = {
            "type": "error_log",
            "module_name": "chatterbox",
            "metric_name":"quick_search",
            "ts":"00",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": [],
            "report_message": "Report description for test No. 5"
        }

        status = pms.insert_error_log_report(report)
        self.assertFalse(status)

#-----------------------------------------------------------------------------------------

    def test_insert_timeliness_report_success(self):
        report = {
            "type": "timeliness",
            "module_name": "chatterbox",
            "metric_name":"quick_search",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": [],
            "ts":"2019-09-09 09:09:00",
            "execution_time": str(random.randint(1,21)*25)
        }

        status = pms.insert_timeliness_report(report)
        self.assertFalse(status)

    def test_insert_timeliness_report_success_existing_module(self):
        report = {
            "type": "timeliness",
            "module_name": "chatterbox",
            "metric_name":"quick_search",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": [],
            "ts":"2019-09-09 09:09:00",
            "execution_time": str(random.randint(1,21)*25)
        }
        
        status = pms.insert_timeliness_report(report)
        self.assertTrue(status)


    def test_insert_timeliness_report_success_existing_metric(self):
        report = {
            "type": "timeliness",
            "module_name": "chatterbox",
            "metric_name":"quick_search",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": [],
            "ts":"2019-09-09 09:09:00",
            "execution_time": str(random.randint(1,21)*25)
        }
        
        status = pms.insert_timeliness_report(report)
        self.assertTrue(status)

    def test_insert_timeliness_report_success_utf8_character(self):
        report = {
            "type": "timeliness",
            "module_name": "chatterbox",
            "metric_name":"quick_search",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": [],
            "ts":"2019-09-09 09:09:00",
            "execution_time": str(random.randint(1,21)*25)
        }
        
        status = pms.insert_timeliness_report(report)
        self.assertTrue(status)

    def test_insert_timeliness_report_success_with_sub_metrics(self):
        report = {
            "type": "timeliness",
            "module_name": "chatterbox",
            "metric_name":"quick_search",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": ['test1'],
            "ts":"2019-09-09 09:09:00",
            "execution_time": "100"
        }
        
        status = pms.insert_timeliness_report(report)
        self.assertTrue(status)

    def test_insert_timeliness_report_fail_invalid_metric(self):
        report = {
            "type": "timeliness",
            "module_name": "chatterbox",
            "metric_name":"quick_seaasdasdasdasdrch",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": [],
            "ts":"2019-09-09 09:09:00",
            "execution_time": "100"
        }

        status = pms.insert_timeliness_report(report)
        self.assertFalse(status)

    def test_insert_timeliness_report_fail_invalid_execution_time_length(self):
        report = {
            "type": "timeliness",
            "module_name": "chatterbox",
            "metric_name":"quick_search",
            "reference_id": '1',
            "reference_table": 'smsoutbox',
            "submetrics": [],
            "ts":"2019-09-09 09:09:00",
            "execution_time": str(random.randint(1,21)*2599999999999999999999999999)
        }

        status = pms.insert_timeliness_report(report)
        self.assertFalse(status)

    def test_insert_timeliness_report_fail_empty_fields(self):
        report = {
            "type": "timeliness",
            "module_name": "",
            "metric_name":"",
            "reference_id": '',
            "reference_table": '',
            "submetrics": [],
            "ts":"",
            "execution_time": ""
        }

        status = pms.insert_timeliness_report(report)
        self.assertFalse(status)

suite = unittest.TestLoader().loadTestsFromTestCase(TestPMSLib)
unittest.TextTestRunner(verbosity=2).run(suite)