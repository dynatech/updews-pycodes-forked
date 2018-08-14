import unittest
from gsm import smsparser2
import smsclass

class TestModule(unittest.TestCase):
    sms = smsclass.SmsInbox(inbox_id=12345,msg="",
            sim_num="639171234567",ts="2018-01-02 03:04:05")    

    def test_v1_use_valid_v1_data_exp_success(self):
        # args = {
        #   "resource": "sms_data",
        #   "host": "local"
        # }
        # args = ("","","sms_data",1)
        self.sms.msg = ("LABBDUE*013E002901A09B0023C401007009E4033F8012FD70A83043E9"
            "00BFFE0AA5053CA0050310A4E0643C06BF8F09D2073F201B0030A83083F200601C"
            "0BCE093EA02AFFF09C2*180730100250")
        status = smsparser2.subsurface.v1(self.sms)
        self.assertIsNotNone(status)

    def test_v1_use_prefix_only_exp_raised_exception(self):
        # args = {
        #   "resource": "sms_data",
        #   "host": "local"
        # }
        # args = ("","","sms_data",1)
        self.sms.msg = ("LABBDUE")
        with self.assertRaises(ValueError) as context:
            smsparser2.subsurface.v1(self.sms)

        self.assertTrue('Wrong message construction' in context.exception)

    def test_v2_use_valid_v2_data_exp_success(self):
        self.sms.msg = ("GAATC*y*250CFD3EEFC8F7C260CFB302003086270C004E4FF5F81280C05"
            "4C6FE0F84*180727120150")
        status = smsparser2.subsurface.v2(self.sms)
        self.assertIsNotNone(status)

    def test_v2_use_valid_v2_data_w_soms_exp_success(self):
        self.sms.msg = ("GAASA*x*0A0B0843D0F9F830B0B014270F8F840C0BFF32301C0810D0B04"
            "0000E0F82*180727120225")
        status = smsparser2.subsurface.v2(self.sms)
        self.assertIsNotNone(status)

    def test_observation_use_valid_routine_msg_exp_success(self):
        self.sms.msg = ("Routine ina july 27 2018 8:45am a 97.1cm b 61.5cm c 66cm d "
            "17cm e 9.5cm f 33.8cm g 85.1cm maulan neridelacruz bernalyoresco m"
            "eldridbenola rosamolina") 
        status = smsparser2.surficial.observation(self.sms.msg)
        self.assertIsNotNone(status)

    def test_observation_use_invalid_site_code_sms_exp_raise_exception_w_non_zero_value(self):
        self.sms.msg = ("Routine fail july 27 2018 8:45am a 97.1cm b 61.5cm c 66cm d "
            "17cm e 9.5cm f 33.8cm g 85.1cm maulan neridelacruz bernalyoresco m"
            "eldridbenola rosamolina") 
        
        with self.assertRaises(ValueError) as err_val:
            smsparser2.surficial.observation(self.sms.msg)

        self.assertTrue(err_val>0)

    def test_observation_use_invalid_date_sms_exp_raise_exception_w_non_zero_value(self):
        self.sms.msg = ("Routine ime julay 27 2018 8:45am a 97.1cm b 61.5cm c 66cm d "
            "17cm e 9.5cm f 33.8cm g 85.1cm maulan neridelacruz bernalyoresco m"
            "eldridbenola rosamolina") 
        
        with self.assertRaises(ValueError) as err_val:
            smsparser2.surficial.observation(self.sms.msg)

        self.assertTrue(err_val>0)

    def test_observation_use_invalid_time_sms_exp_raise_exception_w_non_zero_value(self):
        self.sms.msg = ("Routine ime july 27 2018 28:45am a 97.1cm b 61.5cm c 66cm d "
            "17cm e 9.5cm f 33.8cm g 85.1cm maulan neridelacruz bernalyoresco m"
            "eldridbenola rosamolina") 

        with self.assertRaises(ValueError) as err_val:
            smsparser2.surficial.observation(self.sms.msg)

        self.assertTrue(err_val>0)

    def test_observation_use_no_valid_measurement_sms_exp_raise_exception_w_non_zero_value(self):
        self.sms.msg = ("Routine ime july 27 2018 8:45am maulan neridelacruz bernalyoresco m"
            "eldridbenola rosamolina") 

        with self.assertRaises(ValueError) as err_val:
            smsparser2.surficial.observation(self.sms.msg)

        self.assertTrue(err_val>0)

    def test_observation_use_invalid_weather_description_sms_exp_raise_exception_w_non_zero_value(self):
        self.sms.msg = ("Routine ime july 27 2018 8:45am a 97.1cm b 61.5cm c 66cm d "
            "17cm e 9.5cm f 33.8cm g 85.1cm mapaso neridelacruz bernalyoresco m"
            "eldridbenola rosamolina") 

        with self.assertRaises(ValueError) as err_val:
            smsparser2.surficial.observation(self.sms.msg)

        self.assertTrue(err_val>0)

    def test_uts_use_valid_uts_extenso_data_exp_success(self):
        self.sms.msg = "INAXA*U*LA:11891*MX:330*MI:3392*TP:23.31*180709213054"
        status = smsparser2.extensometer.uts(self.sms)
        self.assertIsNotNone(status)        

    def test_uts_use_invalid_timestamp_data_exp_raise_exception(self):
        self.sms.msg = "INAXA*U*LA:11891*MX:330*MI:3392*TP:23.31*1807092"

        with self.assertRaises(ValueError) as err_val:
            smsparser2.extensometer.uts(self.sms)

        self.assertTrue("No valid timestamp recognized" in err_val.exception)

    def test_rain_arq_use_valid_arq_data_exp_success(self):
        self.sms.msg = "ARQ+0+0+4.158+4.204+0.1268+5.088+0.121+1000+22+31.5+84.1+233+180801/083159"
        self.sms.sim_num = "639175015138"
        status = smsparser2.rain.rain_arq(self.sms)
        self.assertIsNotNone(status)

    def test_rain_arq_use_unregistered_number_arq_data_exp_raise_exception(self):
        self.sms.msg = "ARQ+0+0+4.158+4.204+0.1268+5.088+0.121+1000+22+31.5+84.1+233+180801/083159"
        self.sms.sim_num = "639171234567"

        with self.assertRaises(ValueError) as err_val:
            smsparser2.rain.rain_arq(self.sms)

        print err_val.exception

        self.assertTrue("Number not registered" in err_val.exception)

    def test_rain_arq_use_incomplete_arq_data_exp_raise_exception(self):
        self.sms.msg = "ARQ+22+31.5+84.1+233+180801/083159"
        self.sms.sim_num = "639175015138"
        
        with self.assertRaises(ValueError) as err_val:
            smsparser2.rain.rain_arq(self.sms)

        print err_val.exception

        self.assertTrue("Incomplete data" in err_val.exception)

    def test_v3_use_valid_gateway_rain_data_exp_success(self):
        self.sms.msg = "IMUW,07/27/18,12:15:04,0,000,000,0.00,13.03,11"
        status = smsparser2.rain.v3(self.sms)
        self.assertIsNotNone(status)

    def test_v3_use_invalid_timestamp_rain_data_exp_fail(self):
        self.sms.msg = "IMUW,17/27/18,12:15:04,0,000,000,0.00,13.03,11"
        status = smsparser2.rain.v3(self.sms)
        self.assertTrue(not status)

    def test_v3_use_inincomple_rain_data_exp_fail(self):
        self.sms.msg = "IMUW,07/27/18,12:15:04,0"
        status = smsparser2.rain.v3(self.sms)
        self.assertTrue(not status)

    def test_eq_use_valid_earthquake_sms_exp_success(self):
        self.sms.msg = ("EQInfo1: 20Mar2018 04:05AM Ms=2.5 D=046km 08.26N, 126."
            "02E - 011km N20E of Bunawan (Agusan Del Sur) <RAP>")
        status = smsparser2.earthquake.eq(self.sms)
        self.assertTrue(status)

    def test_eq_use_invalid_timestamp_earthquake_sms_exp_fail(self):
        self.sms.msg = ("EQInfo1: 20XXX2018 04:05AM Ms=2.5 D=046km 08.26N, 126."
            "02E - 011km N20E of Bunawan (Agusan Del Sur) <RAP>")
        status = smsparser2.earthquake.eq(self.sms)
        self.assertTrue(not status)

    def test_eq_use_no_magnitude_earthquake_sms_exp_fail(self):
        self.sms.msg = ("EQInfo1: 20Mar2018 04:05AM D=046km 08.26N, 126."
            "02E - 011km N20E of Bunawan (Agusan Del Sur) <RAP>")
        status = smsparser2.earthquake.eq(self.sms)
        self.assertTrue(not status)


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestModule)
    unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    main()
