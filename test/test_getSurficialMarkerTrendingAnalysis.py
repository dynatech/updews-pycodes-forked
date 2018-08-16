from web_plots import getSurficialMarkerTrendingAnalysis as surficial
import unittest
import pandas as pd

class TestModule(unittest.TestCase):
    unittest.TestCase.maxDiff = None
    
    def test_trending_return_json_pos1(self):
        fp = open("surficial_marker_trending_return_1.json", "r")
        expected_json = fp.read()
        expected_json = expected_json[:-1]
        return_json = surficial.get_marker_trending_analysis_json(site_id=1,
                                                    marker_id=197,
                                                    ts="2018-05-09 00:00:00")
        
        self.assertMultiLineEqual(expected_json, return_json, "True positive test failed")
    
    def test_trending_return_json_pos2(self):
        fp = open("surficial_marker_trending_return_2.json", "r")
        expected_json = fp.read()
        expected_json = expected_json[:-1]
        return_json = surficial.get_marker_trending_analysis_json(site_id=1,
                                                    marker_id=73,
                                                    ts="2018-05-09 00:00:00")
        
        self.assertMultiLineEqual(expected_json, return_json, "True positive test failed")
        
    def test_trending_return_json_neg1(self):
        expected_json = "false_Str"
        return_json = surficial.get_marker_trending_analysis_json(site_id=1,
                                                    marker_id=197,
                                                    ts="2018-05-09 00:00:00")
        
        self.assertMultiLineEqual(expected_json, return_json, "True negative test success")
        
    def test_trending_return_json_neg2(self):
        expected_json = "false_Str"
        return_json = surficial.get_marker_trending_analysis_json(site_id=1,
                                                    marker_id=73,
                                                    ts="2018-05-09 00:00:00")
        
        self.assertMultiLineEqual(expected_json, return_json, "True positive test failed")
        
    def test_has_web_plots_str1(self):
        return_json = surficial.get_marker_trending_analysis_json(site_id=1,
                                                    marker_id=197,
                                                    ts="2018-05-09 00:00:00")
        self.assertRegexpMatches("web_plots=", return_json, "str `web_plots=` not found on return string")
        
def main():
	suite = unittest.TestLoader().loadTestsFromTestCase(TestModule)
	unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    main()