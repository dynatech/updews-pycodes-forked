import unittest
from gsm import smstables
from dynadb import db as dbio

class TestDb(unittest.TestCase):

    def test_connect(self):
        # args = {
        #   "resource": "sms_data",
        #   "host": "local"
        # }
        # args = ("","","sms_data",1)
        status = dbio.connect(resource="sms_data")
        self.assertIsNotNone(status)

    def test_connect_2(self):

        status = dbio.connect(host="local")
        self.assertIsNotNone(status)

    def test_connect_3(self):

        status = dbio.connect(connection="sb_local")
        self.assertIsNotNone(status)

    def test_read(self):

        status = dbio.read(query="select * from sites", resource="sensor_data")
        self.assertIsNotNone(status)

    def test_read_2(self):

        status = dbio.read(query="select * from sites_null", resource="sensor_data")
        self.assertIsNone(status)


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDb)
    unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    main()
