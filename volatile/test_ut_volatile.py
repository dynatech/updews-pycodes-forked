import unittest
from volatile import config
#import config

class TestModule(unittest.TestCase):

    def test_set_cnf_use_valid_cnf_file_exp_success(self):
        status = config.set_cnf("connections.cnf","test_connection")
        self.assertTrue(status)

    def test_set_cnf_use_none_existent_cnf_file_exp_success(self):
        with self.assertRaises(ValueError) as context:
            config.set_cnf("does_not_exist.cnf","test_connection")
        self.assertTrue('File does not exist:' in str(context.exception))


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestModule)
    unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    main()
