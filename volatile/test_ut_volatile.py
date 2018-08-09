import unittest
from volatile import config
from volatile import memory

class TestModule(unittest.TestCase):

    def test_set_cnf_use_valid_cnf_file_exp_success(self):
        status = config.set_cnf("connections.cnf","test_connection")
        self.assertTrue(status)

    def test_set_cnf_use_none_existent_cnf_file_exp_success(self):
        with self.assertRaises(ValueError) as context:
            config.set_cnf("does_not_exist.cnf","test_connection")
        self.assertTrue('File does not exist:' in str(context.exception))

    def test_get_handle_use_default_exp_success(self):
        self.assertIsNotNone(memory.get_handle())

    def test_get_use_server_config_exp_success(self):
        self.assertIsNotNone(memory.get("server_config"))    

    def test_get_use_non_existent_var_exp_none(self):
        self.assertIsNotNone(not memory.get("not_existent_var"))    

    def test_server_config_use_default_exp_success(self):
        self.assertIsNotNone(memory.server_config())    

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestModule)
    unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    main()
