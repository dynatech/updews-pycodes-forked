import unittest
from gsm import smsparser2
import smsclass

class TestModule(unittest.TestCase):

    def test_v1_1(self):
        # args = {
        #   "resource": "sms_data",
        #   "host": "local"
        # }
        # args = ("","","sms_data",1)
        lgr_msg = ("LABBDUE*013E002901A09B0023C401007009E4033F8012FD70A83043E9"
            "00BFFE0AA5053CA0050310A4E0643C06BF8F09D2073F201B0030A83083F200601C"
            "0BCE093EA02AFFF09C2*180730100250")
        lgr_ts = "2018-07-30 10:05:12"
        sms = smsclass.SmsInbox(inbox_id=12345,msg=lgr_msg,
            sim_num="639171234567",ts=lgr_ts)
        status = smsparser2.subsurface.v1(sms)
        self.assertIsNotNone(status)

    def test_v1_2(self):
        # args = {
        #   "resource": "sms_data",
        #   "host": "local"
        # }
        # args = ("","","sms_data",1)
        lgr_msg = ("LABBDUE")
        lgr_ts = "2018-07-30 10:05:12"
        sms = smsclass.SmsInbox(inbox_id=12345,msg=lgr_msg,
            sim_num="639171234567",ts=lgr_ts)

        with self.assertRaises(ValueError) as context:
            smsparser2.subsurface.v1(sms)

        self.assertTrue('Wrong message construction' in context.exception)

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestModule)
    unittest.TextTestRunner(verbosity=1).run(suite)

if __name__ == "__main__":
    main()
