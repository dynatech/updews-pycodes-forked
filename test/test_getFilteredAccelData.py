from web_plots import getFilteredAccelData as accel
import unittest
import pandas as pd

class TestModule(unittest.TestCase):
    unittest.TestCase.maxDiff = None
    
    def test_accel_return_json_pos1(self):
        fp = open("filtered_accel_return_1.json", "r")
        expected_json = fp.read()
        expected_json = expected_json[:-1]
        return_json = accel.get_filtered_accel_data_json(site_column = "agbta",
                                                         start_date = "2017-11-04 00:00",
                                                         end_date = "2017-11-11 00:00",
                                                         node_id = 1, version = 2)
        
        self.assertMultiLineEqual(expected_json, return_json, "True positive test failed")
    
    def test_accel_return_json_pos2(self):
        fp = open("filtered_accel_return_2.json", "r")
        expected_json = fp.read()
        expected_json = expected_json[:-1]
        return_json = accel.get_filtered_accel_data_json(site_column = "labt",
                                                         start_date = "2018-07-21 00:00",
                                                         end_date = "2018-07-28 00:00",
                                                         node_id = 1, version = 1)
        
        self.assertMultiLineEqual(expected_json, return_json, "True positive test failed")
        
    def test_accel_return_json_pos3(self):
        fp = open("filtered_accel_return_3.json", "r")
        expected_json = fp.read()
        expected_json = expected_json[:-1]
        return_json = accel.get_filtered_accel_data_json(site_column = "tueta",
                                                         start_date = "2018-08-10 00:00",
                                                         end_date = "2018-08-17 00:00",
                                                         node_id = 1, version = 2)
        
        self.assertMultiLineEqual(expected_json, return_json, "True positive test failed")
        
    def test_accel_return_json_neg1(self):
        expected_json = "false_Str"
        return_json = accel.get_filtered_accel_data_json(site_column = "agbta",
                                                         start_date = "2017-11-04 00:00",
                                                         end_date = "2017-11-11 00:00",
                                                         node_id = 1, version = 2)
        
        self.assertMultiLineEqual(expected_json, return_json, "True negative test success")
        
    def test_accel_return_json_neg2(self):
        expected_json = "false_Str"
        return_json = accel.get_filtered_accel_data_json(site_column = "labt",
                                                         start_date = "2018-07-21 00:00",
                                                         end_date = "2018-07-28 00:00",
                                                         node_id = 1,
                                                         version = 1)
        
        self.assertMultiLineEqual(expected_json, return_json, "True negative test success")
        
    def test_accel_return_json_neg3(self):
        expected_json = "false_Str"
        return_json = accel.get_filtered_accel_data_json(site_column = "tueta",
                                                         start_date = "2018-08-10 00:00",
                                                         end_date = "2018-08-17 00:00",
                                                         node_id = 1, version = 2)
        
        self.assertMultiLineEqual(expected_json, return_json, "True negative test success")
        
    def test_has_web_plots_str1(self):
        return_json = accel.get_filtered_accel_data_json(site_column = "labt",
                                                         start_date = "2018-07-21 00:00",
                                                         end_date = "2018-07-28 00:00",
                                                         node_id = 1,
                                                         version = 1)
        self.assertRegexpMatches(return_json, "web_plots=", "str `web_plots=` not found on return string")
        
    def test_has_web_plots_str2(self):
        return_json = accel.get_filtered_accel_data_json(site_column = "agbta",
                                                         start_date = "2017-11-04 00:00",
                                                         end_date = "2017-11-11 00:00",
                                                         node_id = 1, version = 2)
        self.assertRegexpMatches(return_json, "web_plots=", "str `web_plots=` not found on return string")
        
def main():
	suite = unittest.TestLoader().loadTestsFromTestCase(TestModule)
	unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    main()