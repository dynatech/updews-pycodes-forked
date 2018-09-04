import unittest as ut
import eq_alert_gen as eq
import pandas as pd
import json
import ConfigParser
import os

class test(ut.TestCase):
    """
    Test sequence for eq function crit_dist
    """
    def test_critdist_1(self):
        func = eq.crit_dist(4)
        self.assertEqual(func,4.842000000000098)

    def test_critdist_2(self):
        func = eq.crit_dist(5)
        self.assertEqual(func,14.195000000000277)

    def test_critdist_3(self):
        func = eq.crit_dist(6)
        self.assertEqual(func,81.60200000000009)

    def test_critdist_4(self):
        func = eq.crit_dist(7)
        self.assertEqual(func,207.0630000000001)

    def test_critdist_5(self):
        func = eq.crit_dist(8)
        self.assertEqual(func,390.5780000000002)
        
    
    """
    test sequence for eq function get_radius
    """
    def test_getradius_1(self):
        func = eq.get_radius(1)
        self.assertEqual(func,0.008993216059187304)

    def test_getradius_2(self):
        func = eq.get_radius(2)
        self.assertEqual(func,0.01798643211837461)
        
    def test_getradius_3(self):
        func = eq.get_radius(3)
        self.assertEqual(func,0.026979648177561915)

    def test_getradius_4(self):
        func = eq.get_radius(4)
        self.assertEqual(func,0.03597286423674922)

    def test_getradius_5(self):
        func = eq.get_radius(5)
        self.assertEqual(func,0.04496608029593653)


    """
    test sequence for eq function get_sites
    """
    def test_getsites(self):
        func = eq.get_sites()
        df = pd.read_csv('sites_reference.csv', index_col=[0])
        
        pd.testing.assert_frame_equal(func, df,by_blocks=True, 
                                       check_exact=True)
    
    """
    test sequence for eq function get_sites
    """
    def test_getunproc(self):
        func = eq.get_unprocessed()
        self.assertFalse(func.empty)
        

def main():
    suite = ut.TestLoader().loadTestsFromTestCase(test)
    ut.TextTestRunner(verbosity=2).run(suite)
    
if __name__ == "__main__":
    main()