from web_plots import getRainfallDataBySource as rainfall
import unittest
import pandas as pd


class TestGetRainfallDataBySource(unittest.TestCase):
    unittest.TestCase.maxDiff = None

# TRUE POSITIVE TESTS
    
    def test_rainfall_json_success_use_agbta_valid_ts(self):
        fp = open("rainfall_expected_json_1_agbta.json", "r")
        expected_json = fp.read()
        
        return_json = rainfall.get_rainfall_data_by_source_json(
                rain_gauge = "rain_agbta",
                offset = pd.to_datetime("2017-11-01 00:00"),
                start_date = pd.to_datetime("2017-11-04 00:00"),
                end_date = pd.to_datetime("2017-11-11 00:00"))

        self.assertMultiLineEqual(expected_json, return_json, 
                                  "ABGTA TRUE POSITIVE TEST FAILED - " \
                                  "incorrect returned value")
        
    def test_rainfall_json_success_use_bantb_valid_ts(self):
        fp = open("rainfall_expected_json_2_bantb.json", "r")
        expected_json = fp.read()
        
        return_json = rainfall.get_rainfall_data_by_source_json(
                rain_gauge = "rain_bantb",
                offset = pd.to_datetime("2017-11-01 00:00"),
                start_date = pd.to_datetime("2017-11-04 00:00"),
                end_date = pd.to_datetime("2017-11-11 00:00"))

        self.assertMultiLineEqual(expected_json, return_json, 
                                  "BANTB TRUE POSITIVE TEST FAILED - " \
                                  "incorrect returned value")

    def test_rainfall_json_success_use_bantb_ts_has_data(self):        
        return_json = rainfall.get_rainfall_data_by_source_json(
                rain_gauge = "rain_bantb",
                offset = pd.to_datetime("2017-11-01 00:00"),
                start_date = pd.to_datetime("2017-11-04 00:00"),
                end_date = pd.to_datetime("2017-11-11 00:00"))

        self.assertIsNotNone(return_json,
                             "BANTB TRUE POSITIVE TEST FAILED - no data")

    def test_rainfall_json_success_use_labt_valid_ts(self):
        fp = open("rainfall_expected_json_3_labt.json", "r")
        expected_json = fp.read()
        
        return_json = rainfall.get_rainfall_data_by_source_json(
                rain_gauge = "rain_labt",
                offset = pd.to_datetime("2017-03-01 00:00"),
                start_date = pd.to_datetime("2017-03-04 00:00"),
                end_date = pd.to_datetime("2017-03-11 00:00"))

        self.assertMultiLineEqual(expected_json, return_json, 
                                  "LABT TRUE POSITIVE TEST FAILED - incorrect"\
                                  "returned value")

    def test_rainfall_json_success_use_labt_ts_has_data(self):
        return_json = rainfall.get_rainfall_data_by_source_json(
                rain_gauge = "rain_labt",
                offset = pd.to_datetime("2017-03-01 00:00"),
                start_date = pd.to_datetime("2017-03-04 00:00"),
                end_date = pd.to_datetime("2017-03-11 00:00"))

        self.assertIsNotNone(return_json,
                                  "LABT TRUE POSITIVE TEST FAILED - no data")

# TRUE NEGATIVE TESTS

    def test_rainfall_json_fail_use_labt_invalid_offset(self):
        fp = open("rainfall_expected_json_3_labt.json", "r")
        expected_json = fp.read()
        
        return_json = rainfall.get_rainfall_data_by_source_json(
                rain_gauge = "rain_labt",
                offset = pd.to_datetime("2023-11-01 00:00"),
                start_date = pd.to_datetime("2017-11-04 00:00"),
                end_date = pd.to_datetime("2017-11-11 00:00"))

        self.assertNotEqual(expected_json, return_json, 
                                  "LABT True negative test FAILED - compared" \
                                  "json data might be equal")

    def test_rainfall_json_fail_use_agbta_ts_no_data(self):
        return_json = rainfall.get_rainfall_data_by_source_json(
                rain_gauge = "rain_agbta",
                offset = pd.to_datetime("2009-11-01 00:00"),
                start_date = pd.to_datetime("2009-11-04 00:00"),
                end_date = pd.to_datetime("2009-11-11 00:00"))

        self.assertIn("null", return_json, 
                                  "AGBTA TRUE NEGATIVE TEST FAILED - json " \
                                  "data should be null.") 

    def test_rainfall_json_fail_use_bakg_ts_no_data(self):      
        return_json = rainfall.get_rainfall_data_by_source_json(
                rain_gauge = "rain_bakg",
                offset = pd.to_datetime("2017-11-01 00:00"),
                start_date = pd.to_datetime("2017-11-04 00:00"),
                end_date = pd.to_datetime("2017-11-11 00:00"))

        self.assertIn("null", return_json, 
                                  "BAKG TRUE NEGATIVE TEST FAILED - json " \
                                  "data should be null.")
        
    def test_rainfall_json_fail_use_rain_gauge_invalid(self):      
        return_json = rainfall.get_rainfall_data_by_source_json(
                rain_gauge = "rain_bantb",
                offset = pd.to_datetime("2017-11-01 00:00"),
                start_date = pd.to_datetime("2017-11-04 00:00"),
                end_date = pd.to_datetime("2017-12-11 00:00"))

        self.assertIn("null", return_json, 
                                  "BAKG TRUE NEGATIVE TEST FAILED - json " \
                                  "data should be null.")

# MISC TESTS        
    def test_rainfall_has_web_plots(self):
        return_json = rainfall.get_rainfall_data_by_source_json(
                rain_gauge = "rain_agbta",
                offset = pd.to_datetime("2017-11-01 00:00"),
                start_date = pd.to_datetime("2017-11-04 00:00"),
                end_date = pd.to_datetime("2017-11-11 00:00"))
        self.assertIn("web_plots=",return_json)
        
def main():
	suite = unittest.TestLoader().loadTestsFromTestCase(
            TestGetRainfallDataBySource)
	unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    main()