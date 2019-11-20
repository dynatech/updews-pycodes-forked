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

class UserSMS:
    def __init__(self, gsm_mod, db, gsm_id):
        print("<< Imported User SMS")
        today = dt.today()
        self.gsm_mod = gsm_mod
        self.db = db
        self.gsm_id = gsm_id
        self.error_logger = err_log.ErrorLogger(gsm_id, 'User')
        csq = gsm_mod.get_csq()
        db.write_csq(gsm_id, today, csq)
        print(">> CSQ:", csq)
    
    def start_server(self):
        print(">> Starting Users SMS server.")
        while(True):
            today = dt.today()
            if (today.minute % 10 == 0):
                csq = self.gsm_mod.get_csq()
                self.db.write_csq(self.gsm_id, today, csq)
                print(">> CSQ:", csq)
            sms_count = self.gsm_mod.count_sms()
            print(">> Message count(s): ",sms_count)
            if sms_count > 0:
                sms = self.fetch_inbox()
                if len(sms) > 0:
                    print("<< Storing inbox to database.")
                    for x in sms:
                        store_status = self.db.insert_inbox_sms(x.simnum, x.data, x.dt )
                        if not store_status:
                            print(">> Unknown mobile number. Storing mobile number for future reference.")
                            store_number = self.insert_unknown_number(x.simnum, x.data, x.dt)
                else:
                    print(">> No new message. Sleeping for 10 seconds.\n\n")
                    time.sleep(10)
                self.gsm_mod.delete_sms()

            pending_sms = self.fetch_pending_user_outbox()

            if len(pending_sms) > 0:
                print(">> Pending message(s): ",len(pending_sms),"")
                self.send_pending_sms(pending_sms)
            else:
                print(">> No pending message. Sleeping for 10 seconds.\n\n")
                time.sleep(10)
    
    def fetch_inbox(self):
        print("<< Fetching inbox.")
        sms = []
        sms = self.gsm_mod.get_all_sms()
        return sms

    def fetch_pending_user_outbox(self):
        print("<< Fetching outbox.")
        pending = self.db.get_all_outbox_sms_users_from_db(5, self.gsm_id)
        return pending
    
    def send_pending_sms(self, sms):
        for (user_id, mobile_id, outbox_id, stat_id,
         sim_num, firstname, lastname, sms_msg, send_status) in sms:
            print("<< Sending SMS to: ", firstname, lastname, " (",sim_num,")")
            print("<< Message: ", sms_msg)
            stat = self.gsm_mod.send_sms(sms_msg, sim_num)
            if stat is 0:
                print(">> SMS ID #:",outbox_id," has successfully sent!")
            else:
                print(">> SMS ID #:",outbox_id," failed! Will retry later...")
            update_status = self.update_outbox_status(stat, outbox_id, stat_id, send_status)
            if update_status != 1:
                print(">> Error occurred. Failed to update send status.")
    
    def update_outbox_status(self, stat, outbox_id , stat_id, send_status, ts_send = ""):
        ts_sent = dt.today().strftime("%Y-%m-%d %H:%M:%S")
        if stat == 0:
            ret_val = self.db.update_send_status(stat_id , 5, ts_sent)
        else:
            if send_status == 4:
                send_status = send_status + 2
            else:
                send_status = send_status + 1
            ret_val = self.db.update_send_status(stat_id , send_status)
        return ret_val
    
    def insert_unknown_number(self, sim_num, msg, ts):
        print("<< Inserting unknown number.")
        store_number = self.db.save_unknown_number(sim_num, self.gsm_id)
        store_status = self.db.insert_inbox_sms(sim_num, msg, ts)