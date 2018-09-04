from web_plots import getColumnPositionAndDisplacementVelocity as subsurface
import unittest
import pandas
import sys

class TestGetColumnPositionAndDisplacementVelocity(unittest.TestCase):
    unittest.TestCase.maxDiff = None
    # True positive tests
    def test_subsurface_return_json_true_positive_success_1(self):
        file = open("subsurface_return_1.json", "r")
        expected_json = file.read()
        return_json = subsurface.get_vcd_data_json(site_column="agbta",\
        	end_ts="2017-11-11 06:00:00", start_ts="2017-11-08 06:00:00")

        self.assertMultiLineEqual(expected_json, return_json, "True positive failed")
        
    def test_subsurface_return_json_true_positive_success_2(self):
        fp = open("subsurface_return_2.json", "r")
        expected_json = fp.read()
        return_json = subsurface.get_vcd_data_json(site_column="magta",\
        	end_ts="2017-07-11 06:00:00", start_ts="2017-07-08 06:00:00")
        
        self.assertMultiLineEqual(expected_json, return_json, "True positive failed")

    def test_subsurface_return_json_true_positive_success_3(self):
        fp = open("subsurface_return_3.json", "r")
        expected_json = fp.read()
        return_json = subsurface.get_vcd_data_json(site_column="jorta",\
        	end_ts="2017-03-11 06:00:00", start_ts="2017-03-08 06:00:00")
        
        self.assertMultiLineEqual(expected_json, return_json, "True positive failed")

    def test_subsurface_return_json_has_data_1(self):
    	return_json = subsurface.get_vcd_data_json(site_column="jorta",\
        	end_ts="2017-03-11 06:00:00", start_ts="2017-03-08 06:00:00")
        
        self.assertIsNotNone(return_json, "True positive failed - no data")

    def test_subsurface_return_json_has_data_2(self):
    	return_json = subsurface.get_vcd_data_json(site_column="agbta",\
        	end_ts="2017-11-11 06:00:00", start_ts="2017-11-08 06:00:00")
        
        self.assertIsNotNone(return_json, "True positive failed - no data")

    # True negative tests
    def test_subsurface_return_json_true_negative_success_1(self):
        expected_json = "false_string"
        return_json = subsurface.get_vcd_data_json(site_column="magta",\
        	end_ts="2017-07-11 06:00:00", start_ts="2017-07-08 06:00:00")
        
        self.assertNotEqual(expected_json, return_json, "True negative failed")
    
    def test_subsurface_return_json_true_negative_success_2(self):
        expected_json = "false_string"
        return_json = subsurface.get_vcd_data_json(site_column="jorta",\
        	end_ts="2017-03-11 06:00:00", start_ts="2017-03-08 06:00:00")
        
        self.assertNotEqual(expected_json, return_json, "True negative failed")

    def test_subsurface_return_json_true_negative_success_3(self):
        return_json = subsurface.get_vcd_data_json(site_column="jorta",\
        	end_ts="2007-03-11 06:00:00", start_ts="2007-03-08 06:00:00")
        
        self.assertIn("0.0", return_json, "True negative failed")
        
    def test_subsurface_return_json_true_negative_success_4(self):
        return_json = subsurface.get_vcd_data_json(site_column="agbta",\
        	end_ts="2009-03-11 06:00:00", start_ts="2009-03-08 06:00:00")
        
        self.assertIn("0.0", return_json, "True negative failed")        
    
    # true positive check if has web plots
    def test_subsurface_has_web_plots(self):
        return_json = subsurface.get_vcd_data_json(site_column="agbta",\
        	end_ts="2017-11-11 06:00:00", start_ts="2017-03-08 06:00:00")

        self.assertIn("web_plots=", return_json)
        
def main():
	suite = unittest.TestLoader().loadTestsFromTestCase(TestGetColumnPositionAndDisplacementVelocity)
	unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    main()