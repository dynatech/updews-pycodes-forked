import argparse
import os
import serial
import sys
import warnings
import unittest

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from gsm.modem import modem
from volatile import memory as mem


class TestModule(unittest.TestCase):

    GSM_ID = 99
    GSM_MODULES = None
    BAUDRATE = 57600
    GSM = None
    # check if gsm_modules variable is in memory
    def test_gsm_module_reference_in_memory(self):
        self.assertIsNotNone(self.GSM_MODULES)

    def test_check_if_gsm_id_is_ok(self):
        self.assertTrue(self.GSM_ID < 50)

    def test_GsmModem_class_use_argument_gsm_id_exp_true(self):
        self.assertIsNotNone(self.GSM)

    def test_GsmModem_class_use_invalid_gsm_info_exp_fail(self):
        with self.assertRaises(serial.serialutil.SerialException) as context:
            modem.GsmModem("invalid_port", self.BAUDRATE, 99, 99)

        self.assertTrue("could not open port" in str(context.exception))

    def test_set_defaults_use_default_exp_success(self):
        self.assertTrue(self.GSM.set_defaults())



def get_arguments():
    parser = argparse.ArgumentParser(description=("Run unit test for gsm modem"
        " [-options]"))
    parser.add_argument("-g", "--gsm_id", type = int,
        help="gsm id (1,2,3...)", required=True)
    
    try:
        args = parser.parse_args()

        return args        
    except IndexError:
        print ('>> Error in parsing arguments')
        error = parser.format_help()
        print (error)
        sys.exit()

def main():
    warnings.warn("This test is intended to check gsm modem connectivity. "
        "This will only work for hosts with connected gsm modems.")
    args = get_arguments()

    gsm_modules = mem.get_handle().get("gsm_modules")
    gsm_info = gsm_modules[args.gsm_id]
    gsm = modem.GsmModem(gsm_info['port'], 57600, 
            gsm_info["pwr_on_pin"], gsm_info["ring_pin"])

    TestModule.GSM_ID = args.gsm_id
    TestModule.GSM = gsm
    TestModule.GSM_MODULES = gsm_modules

    suite = unittest.TestLoader().loadTestsFromTestCase(TestModule)
    unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    main()
