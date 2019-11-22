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
import raw_sync_parser as raw_parser
import loggers
import users 
from pprint import pprint
import utils.error_logger as err_log
import traceback


class GsmServer:

	def get_arguments(self):
		parser = argparse.ArgumentParser(
			description="Run SMS server [-options]")
		parser.add_argument("-t", "--table",
							help="smsinbox table (loggers or users)")
		parser.add_argument("-db", "--dbhost",
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


if __name__ == "__main__":
	start_time = time.time()
	initialize_gsm = GsmServer()
	args = initialize_gsm.get_arguments()
	os.makedirs(os.path.abspath('./gsm_logs/'), exist_ok=True)
	if args.dbhost is not None:
		dbhost = args.dbhost
	else:
		dbhost = None
	error_logger = err_log.ErrorLogger(args.gsm_id, 'Runner')
	db = dbLib.DatabaseConnection(dbhost, args.gsm_id)
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
		if args.table == "loggers":
			sms_mode = loggers.LoggerSMS(initialize_gsm_modules, db, args.gsm_id)
		elif args.table == "users":
			sms_mode = users.UserSMS(initialize_gsm_modules, db, args.gsm_id)
		else:
			sms_mode = None
			print(">> Unknown sms mode. Exiting...")
			sys.exit()
	except Exception as e:
		print(">> ",e," error occurred. Exiting...")

	try:
		sms_mode.start_server()
	except Exception as e:
		print(">> ",e,"error occurred. Exiting...")