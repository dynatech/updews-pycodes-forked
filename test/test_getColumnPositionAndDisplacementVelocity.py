from web_plots import getColumnPositionAndDisplacementVelocity as subsurface
import unittest
import pandas
import sys

class TestGetColumnPositionAndDisplacementVelocity(unittest.TestCase):
    unittest.TestCase.maxDiff = None
    
    def test_subsurface_return_json_true_positive_success_1(self):
        file = open("subsurface_return_1.json", "r")
        expected_json = file.read()
        return_json = subsurface.get_vcd_data_json(site_column="agbta",\
        	end_ts="2017-11-11 06:00:00", start_ts="2017-11-08 06:00:00")

        self.assertMultiLineEqual(expected_json, return_json, "True positive success")
        
    def test_subsurface_return_json_true_positive_success_2(self):
        fp = open("subsurface_return_2.json", "r")
        expected_json = fp.read()
        return_json = subsurface.get_vcd_data_json(site_column="magta",\
        	end_ts="2017-07-11 06:00:00", start_ts="2017-07-08 06:00:00")
        
        self.assertMultiLineEqual(expected_json, return_json, "True positive success")
        
    def test_subsurface_return_json_true_negative_fail_1(self):
        expected_json = "false_string"
        return_json = subsurface.get_vcd_data_json(site_column="magta",\
        	end_ts="2017-07-11 06:00:00", start_ts="2017-07-08 06:00:00")
        
        self.assertNotEqual(expected_json, return_json, "True negative success")
        
    def test_subsurface_return_json_true_negative_fail_2(self):
        expected_json = "false_string"
        return_json = subsurface.get_vcd_data_json(site_column="jorta",\
        	end_ts="2017-03-11 06:00:00", start_ts="2017-03-08 06:00:00")
        
        self.assertNotEqual(expected_json, return_json, "True negative success")
        
    def test_subsurface_has_web_plots(self):
        return_json = subsurface.get_vcd_data_json(site_column="agbta",\
        	end_ts="2017-11-11 06:00:00", start_ts="2017-03-08 06:00:00")

        self.assertIn("web_plots=", return_json)
        
def main():
	suite = unittest.TestLoader().loadTestsFromTestCase(TestGetColumnPositionAndDisplacementVelocity)
	unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    main()