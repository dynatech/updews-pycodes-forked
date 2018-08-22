from web_plots import getColumnPositionAndDisplacementVelocity as subsurface
import unittest
import pandas


class TestGetColumnPositionAndDisplacementVelocity(unittest.TestCase):
    unittest.TestCase.maxDiff = None
    
    def test_subsurface_return_json_pos1(self):
        fp = open("subsurface_return_1.json", "r")
        expected_json = fp.read()
        expected_json = expected_json[:-1]
        return_json = subsurface.get_vcd_data_json(site_column="agbta",end_ts="2017-11-11 06:00:00", start_ts="2017-03-08 06:00:00")
        
        self.assertMultiLineEqual(expected_json, return_json, "True positive test failed")
        
    def test_subsurface_return_json_pos2(self):
        fp = open("subsurface_return_2.json", "r")
        expected_json = fp.read()
        expected_json = expected_json[:-1]
        return_json = subsurface.get_vcd_data_json(site_column="magta",end_ts="2017-07-11 06:00:00", start_ts="2017-07-08 06:00:00")
        
        self.assertMultiLineEqual(expected_json, return_json, "True positive test failed")
        
    def test_subsurface_return_json_neg1(self):
        expected_json = "false_string"
        return_json = subsurface.get_vcd_data_json(site_column="magta",end_ts="2017-07-11 06:00:00", start_ts="2017-07-08 06:00:00")
        
        self.assertMultiLineEqual(expected_json, return_json, "True negative test success")
        
    def test_subsurface_return_json_neg2(self):
        expected_json = "false_string"
        return_json = subsurface.get_vcd_data_json(site_column="jorta",end_ts="2017-03-11 06:00:00", start_ts="2017-03-08 06:00:00")
        
        self.assertMultiLineEqual(expected_json, return_json, "True negative test failed")
        
    def test_subsurface_has_web_plots(self):
        return_json = subsurface.get_vcd_data_json(site_column="agbta",end_ts="2017-11-11 06:00:00", start_ts="2017-03-08 06:00:00")
        self.assertRegexMatches("web_plots=", return_json, "str `web_plots=` not found on return string")
        
def main():
	suite = unittest.TestLoader().loadTestsFromTestCase(TestGetColumnPositionAndDisplacementVelocity)
	unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    main()