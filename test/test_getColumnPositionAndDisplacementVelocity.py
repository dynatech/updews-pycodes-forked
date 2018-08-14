
import sys
sys.path.insert(1, "/var/www/updews-pycodes/web_plots")
import getColumnPositionAndDisplacementVelocity as subsurface
import unittest
import pandas


class TestGetColumnPositionAndDisplacementVelocity(unittest.TestCase):
	def initialize_vcd_argument():
		subsurface.getDF("agbta", "2017-11-11 06:00:00", "2017-11-08 06:00:00")

	def main():
		initialize_vcd_argument()
		suite = unittest.TestLoader().loadTestsFromTestCase(TestGetColumnPositionAndDisplacementVelocity)
		unittest.TextTestRunner(verbosity=2).run(suite)

	main()