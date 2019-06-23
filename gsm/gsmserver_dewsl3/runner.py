import os
import time
import serial
import re
import sys
import configparser
from datetime import datetime as dt
import memcache
import argparse
import gsm_modules as modem
import db_lib as dbLib
from pprint import pprint


class GsmServer:

	def get_arguments(self):
		parser = argparse.ArgumentParser(
			description="Run SMS server [-options]")
		parser.add_argument("-t", "--table",
							help="smsinbox table (loggers or users)")
		parser.add_argument("-n", "--network",
							help="network name (smart/globe/simulate)")
		parser.add_argument("-g", "--gsm_id", type=int,
							help="gsm id (1,2,3...)")

		try:
			args = parser.parse_args()
			return args
		except IndexError:
			print('>> Error in parsing arguments')
			error = parser.format_help()
			print(error)
			sys.exit()

	def get_gsm_modules(self, gsm_id):
		gsm_modules = db.get_gsm_info(gsm_id)
		return gsm_modules

	def try_sending_messages(self, gsm_mod, gsm_info):
		start = dt.now()
		self.send_messages_from_db(gsm_mod, table='users', send_status=5,
								   gsm_info=gsm_info)

	def run_server(self, gsm_mod, gsm_info, table='users'):
		minute_of_last_alert = dt.now().minute
		timetosend = 0
		lastAlertMsgSent = ''
		logruntimeflag = True
		checkIfActive = True
		print(">> GSM Server Active:", gsm_info['name'])
		print(">> CSQ:", gsm_mod.get_csq())
		print(">> Initialization duration:",
			  (time.time() - start_time), " seconds")
		while True:
			m = gsm_mod.count_sms()
			if m > 0:
				allmsgs = gsm_mod.get_all_sms()
				try:
					db.write_inbox(allmsgs, gsm_info, gsm_mod.get_csq())
					print(">> Writing SMS to Database...")
				except KeyboardInterrupt:
					print(">> Error: May be an empty line.. skipping message storing")

				gsm_mod.delete_sms(gsm_info["module"])

				print(dt.today().strftime(
					">> Server active as of: %A, %B %d, %Y, %X"))
				csq = gsm_mod.get_csq()
				print(">> CSQ:", csq)
				self.try_sending_messages(gsm_mod, gsm_info)
				db.write_csq(gsm_info['gsm_id'], dt.today().strftime("%Y-%m-%d %H:%M:%S"), csq)
			elif m == 0:
				self.try_sending_messages(gsm_mod, gsm_info)
				today = dt.today()
				if (today.minute % 10 == 0):
					if checkIfActive:
						print(dt.today().strftime(
							">> Server active as of: %A, %B %d, %Y, %X"))
						csq = gsm_mod.get_csq()
						print(">> CSQ:", csq)
						db.write_csq(gsm_info['gsm_id'], dt.today().strftime("%Y-%m-%d %H:%M:%S"), csq)
					checkIfActive = False
				else:
					checkIfActive = True
			elif m == -1:
				serverstate = 'inactive'
				raise modem.ResetException(">> GSM Module inactive")
			elif m == -2:
				raise modem.ResetException(
					">> Error in parsing mesages: No data returned by GSM")
			else:
				raise modem.ResetException(
					">> Error in parsing mesages: Error unknown")

	def send_messages_from_db(self, gsm=None, table='users', send_status=5,
							  gsm_info=None):
		if gsm == None:
			raise ValueError(">> No GSM Instance defined")

		allmsgs = db.get_all_outbox_sms_from_db(table, send_status,
												gsm_info["id"])

		mobile_container = []
		msglist = []
		error_stat_list = []
		if len(allmsgs) <= 0:
			return

		for msg in allmsgs:
			smsItem = modem.GsmSms(msg[0], msg[1], msg[4], '')
			table_mobile = db.get_all_user_mobile(msg[1], mobile_id_flag=True)
			for mobile in table_mobile:
				smsItem = modem.GsmSms(msg[0], mobile[1], msg[4], '')
				msglist.append([smsItem, msg[2], msg[3], msg[1]])
			
		today = dt.today().strftime("%Y-%m-%d %H:%M:%S")

		if len(msglist) == 0:
			print(">> No valid message to send")
			return

		allmsgs = msglist
		status_list = []

		pending_msgs = len(allmsgs)
		print("Pending Messages:", pending_msgs)

		for msg in allmsgs:
			try:
				if (msg[0].data != ""):
					num_prefix = re.match(
						"^ *((0)|(63))9\d\d", msg[0].simnum).group()
					num_prefix = num_prefix.strip()
					ret = gsm.send_sms(msg[0].data, msg[0].simnum.strip())
					today = dt.today().strftime("%Y-%m-%d %H:%M:%S")
					if ret != 0:
						stat = msg[0].num, 1, today, msg[2], msg[1], msg[3]
					else:
						stat = msg[0].num, 5, today, msg[2], msg[1], msg[3]
				else:
					stat = msg[0].num, 6, today, msg[2], msg[1], msg[3]
				status_list.append(stat)
				pending_msgs = pending_msgs - 1
				print(">> ",pending_msgs," messages left flagged for sending...")
			except Exception as e:
				print('>> Error:', e)
			print(">> Message:", msg[0].data)
			db.update_sent_status(table, status_list)
			status_list = []

if __name__ == "__main__":
	start_time = time.time()
	initialize_gsm = GsmServer()
	args = initialize_gsm.get_arguments()
	db = dbLib.DatabaseConnection()
	gsm_modules = db.get_gsm_info(args.gsm_id)
	config = configparser.ConfigParser()
	config.read('/home/pi/updews-pycodes/gsm/gsmserver_dewsl3/utils/config.cnf')

	if args.gsm_id not in gsm_modules.keys():
		print(">> Error in gsm module selection (", args.gsm_id, ")")
		sys.exit()

	if gsm_modules[args.gsm_id]["port"] is None:
		print(">> Error: missing information on gsm_module")
		sys.exit()

	gsm_info = gsm_modules[args.gsm_id]
	gsm_info["pwr_on_pin"] = gsm_modules[args.gsm_id]['pwr']
	gsm_info["ring_pin"] = gsm_modules[args.gsm_id]['rng']
	gsm_info["id"] = gsm_modules[args.gsm_id]['gsm_id']
	gsm_info["baudrate"] = int(config['GSM_DEFAULT_SETTINGS']['BAUDRATE'])
	gsm_info["port"] = gsm_modules[args.gsm_id]['port']
	gsm_info["name"] = gsm_modules[args.gsm_id]['gsm_name']
	gsm_info["module"] = gsm_modules[args.gsm_id]['module_type']

	try:
		initialize_gsm_modules = modem.GsmModem(gsm_info['port'], gsm_info["baudrate"],
												gsm_info["pwr_on_pin"], gsm_info["ring_pin"])
	except serial.SerialException:
		print(">> Error: No Ports / Serial / Baudrate detected.")
		serverstate = 'serial'
		raise ValueError(">> Error: no com port found")

	try:
		gsm_defaults = initialize_gsm_modules.set_gsm_defaults()
	except Exception as e:
		print(e)
		print(">> Error: initializing default settings.")
	try:
		initialize_gsm.run_server(initialize_gsm_modules, gsm_info, 'users')
	except modem.ResetException:
		print(">> Resetting system because of GSM failure")
		initialize_gsm_modules.reset()
		time.sleep(int(config['GSM_DEFAULT_SETTINGS']['POWER_RESET_DELAY']))
		initialize_gsm.run_server(initialize_gsm_modules, gsm_info, 'users')
		sys.exit()