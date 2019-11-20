import serial
import time
import re
import RPi.GPIO as GPIO
from gsmmodem import pdu as PduDecoder
from datetime import datetime as dt
from datetime import timedelta as td
import configparser
from pprint import pprint
import sys
import utils.error_logger as err_log
import traceback


class LoggerSMS:
    def __init__(self, gsm_mod, db, gsm_id):
        print("<< Imported Logger SMS")
        today = dt.today()
        self.gsm_mod = gsm_mod
        self.db = db
        self.gsm_id = gsm_id
        self.error_logger = err_log.ErrorLogger(gsm_id, 'Logger')
        csq = gsm_mod.get_csq()
        db.write_csq(gsm_id, today, csq)
        print(">> CSQ:", csq)
    
    def start_server(self):
        print(">> Starting Logger SMS server.")
        try:
            while(True):
                today = dt.today()
                if (today.minute % 10 == 0):
                    csq = self.gsm_mod.get_csq()
                    self.db.write_csq(self.gsm_id, today, csq)
                    print(">> CSQ:", csq)
                sms_count = self.gsm_mod.count_sms()
                print(">> Message count(s): ",sms_count)
                if sms_count > 0:
                    sms = self.fetch_logger_inbox()
                    if len(sms) > 0:
                        for x in sms:
                            store_status = self.db.insert_logger_inbox_sms(x.simnum, x.data, x.dt, self.gsm_id )
                            if not store_status:
                                print(">> Unknown mobile number. Ignoring.")
                            else:
                                print("<< Storing logger inbox to database.")
                    else:
                        print(">> No new message. Sleeping for 10 seconds.\n\n")
                        time.sleep(10)
                    self.gsm_mod.delete_sms()
                else:
                    print(">> No new data. Sleeping for 10 seconds.\n\n")
                    time.sleep(10)
        except Exception as err:
            self.error_logger.store_error_log(self.exception_to_string(err))
    
    def fetch_logger_inbox(self):
        print("<< Fetching logger inbox.")
        sms = []
        sms = self.gsm_mod.get_all_sms()
        return sms

    def exception_to_string(self, excp):
        stack = traceback.extract_stack()[:-3] + traceback.extract_tb(excp.__traceback__)  # add limit=?? 
        pretty = traceback.format_list(stack)
        return ''.join(pretty) + '\n  {} {}'.format(excp.__class__,excp)