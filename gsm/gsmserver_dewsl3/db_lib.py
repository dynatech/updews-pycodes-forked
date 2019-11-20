import MySQLdb
import configparser
from datetime import datetime as dt
from datetime import timedelta as td
from pprint import pprint
import sys
import re
import time
import hashlib
import psutil
import utils.error_logger as err_log
import traceback

class DatabaseCredentials():
	def __new__(self, host):
		config = configparser.ConfigParser()
		config.read('/home/pi/updews-pycodes/gsm/gsmserver_dewsl3/utils/config.cnf')
		if host is not None:
			config["MASTER_DB_CREDENTIALS"]["host"] = host
		return config

class DatabaseConnection():

	db_cred = None
	error_logger = None

	def __init__(self, host, gsm_id):
		self.db_cred = DatabaseCredentials(host)
		self.error_logger = err_log.ErrorLogger(gsm_id, 'Database')

	def get_all_outbox_sms_users_from_db(self, send_status, gsm_id):
		while True:
			try:
				db, cur = self.db_connect(
					self.db_cred['MASTER_DB_CREDENTIALS']['db_comms'])
				query = ("SELECT user_id, mobile_id, outbox_id, stat_id, sim_num, firstname, lastname, sms_msg, send_status FROM smsoutbox_user_status "
						"INNER JOIN smsoutbox_users USING (outbox_id) "
						"INNER JOIN user_mobiles USING (mobile_id) "
						"INNER JOIN mobile_numbers USING (mobile_id) " 
						"INNER JOIN users USING (user_id) WHERE "
						"send_status < %d "
						"AND send_status >= 0 "
						"AND send_status < 6 "
						"AND smsoutbox_user_status.gsm_id = %d "
						"LIMIT 100") % (send_status, gsm_id)

				a = cur.execute(query)
				out = []
				if a:
					out = cur.fetchall()
					db.close()
				return out

			except MySQLdb.OperationalError as err:
				self.error_logger.store_error_log(self.exception_to_string(err))
				print("** MySQL Error occurred. Sleeping for 10 seconds.")
				time.sleep(20)

	def update_send_status(self, stat_id,send_status, ts = None):
		if not ts:
			query = ("UPDATE smsoutbox_user_status SET send_status = %d where stat_id = %d ") % (send_status, stat_id)
		else:
			query = ("UPDATE smsoutbox_user_status SET send_status = %d, ts_sent = '%s' where stat_id = %d ") % (send_status, ts, stat_id)
		return self.write_to_db(query=query, last_insert_id=False)

	def insert_inbox_sms(self, sim_num, msg, ts):
		status = []
		mobile_id = self.get_user_mobile_id(sim_num)
		for ids in mobile_id:
			for id in ids:
				ts_stored = dt.today().strftime("%Y-%m-%d %H:%M:%S")
				query = ("INSERT INTO smsinbox_users VALUES (0,  " 
					"'%s','%s', %d,'%s', " 
					"0)") % (ts, ts_stored, id, msg.replace("'", "\\'"))
				status.append(self.write_to_db(query, last_insert_id=True))
		return status

	def get_user_mobile_id(self, sim_num):
		query = "SELECT mobile_id from mobile_numbers WHERE sim_num like '%"+sim_num[:10]+"%'"
		result = self.read_db(query)
		return result

	def save_unknown_number(self, sim_num, gsm_id):
		query = ("INSERT INTO mobile_numbers VALUES (0, %s, %s)") % (sim_num, gsm_id)
		mobile_id = self.write_to_db(query, last_insert_id=True)
		print(">> Mobile ID:", mobile_id)
		return mobile_id

	def insert_logger_inbox_sms(self, sim_num, data, ts, gsm_id):
		status = []
		mobile_id = self.get_logger_mobile_id(sim_num, gsm_id)
		for ids in mobile_id:
			for id in ids:
				ts_stored = dt.today().strftime("%Y-%m-%d %H:%M:%S")
				query = ("INSERT INTO smsinbox_loggers (ts_sms, ts_stored, mobile_id, "
					"sms_msg,read_status, gsm_id) values ('%s','%s',%s,'%s',0,%s)") % (ts, ts_stored, id, data, gsm_id)
				status.append(self.write_to_db(query, last_insert_id=True))
		return status

	def get_logger_mobile_id(self, sim_num, gsm_id):
		try:
			db, cur = self.db_connect(
				self.db_cred['MASTER_DB_CREDENTIALS']['db_comms'])
			query = ("SELECT mobile_id FROM (SELECT "
						"t1.mobile_id, t1.sim_num, t1.gsm_id "
						"FROM "
						"logger_mobile AS t1 "
						"LEFT OUTER JOIN "
						"logger_mobile AS t2 ON t1.sim_num = t2.sim_num "
						"AND (t1.date_activated < t2.date_activated "
						"OR (t1.date_activated = t2.date_activated "
						"AND t1.mobile_id < t2.mobile_id)) "
						"WHERE "
						"t2.sim_num IS NULL "
						"AND t1.sim_num IS NOT NULL) as logger_mobile WHERE sim_num like '%"+sim_num[:10]+"%' and gsm_id = "+str(gsm_id)+"")
			a = cur.execute(query)
			out = []
			if a:
				out = cur.fetchall()
				db.close()
			return out

		except MySQLdb.OperationalError as err:
			self.error_logger.store_error_log(self.exception_to_string(err))
			time.sleep(20)

	def get_all_user_mobile(self, sim_num, mobile_id_flag=False):
		try:
			db, cur = self.db_connect(
				self.db_cred['MASTER_DB_CREDENTIALS']['db_comms'])
			if mobile_id_flag == False:
				query = "select mobile_id, sim_num, gsm_id from user_mobile where sim_num like '%"+sim_num+"%'"
			else:
				query = "select mobile_id, sim_num, gsm_id from user_mobile where mobile_id = '" + \
					str(sim_num)+"'"
			a = cur.execute(query)
			out = []
			if a:
				out = cur.fetchall()
				db.close()
			return out

		except MySQLdb.OperationalError as err:
			self.error_logger.store_error_log(self.exception_to_string(err))
			print("MySQLdb OP Error:", err)
			time.sleep(20)

	def write_to_db(self, query, last_insert_id=False):
		ret_val = None
		db, cur = self.db_connect(
			self.db_cred['MASTER_DB_CREDENTIALS']['db_comms'])

		try:
			a = cur.execute(query)
			db.commit()
			if last_insert_id:
				b = cur.execute('select last_insert_id()')
				b = str(cur.fetchone()[0]) 
				ret_val = b
			else:
				ret_val = a

		except IndexError as err:
			print("IndexError on ")
			print(str(inspect.stack()[1][3]))
			self.error_logger.store_error_log(self.exception_to_string(err))
		except (MySQLdb.Error, MySQLdb.Warning) as err:
			self.error_logger.store_error_log(self.exception_to_string(err))
			print(">> MySQL error/warning: %s" % e)
			print("Last calls:")
			for i in range(1, 6):
				try:
					print("%s," % str(inspect.stack()[i][3]),)
				except IndexError:
					continue
			print("\n")

		finally:
			db.close()
			return ret_val

	def read_db(self, query):
		try:
			db, cur = self.db_connect(
				self.db_cred['MASTER_DB_CREDENTIALS']['db_comms'])
			a = cur.execute(query)
			out = []
			if a:
				out = cur.fetchall()
				db.close()
			return out

		except MySQLdb.OperationalError as err:
			self.error_logger.store_error_log(self.exception_to_string(err))
			print("MySQLdb OP Error:", err)
			time.sleep(20)

	def db_connect(self, schema):
		try:
			db = MySQLdb.connect(self.db_cred['MASTER_DB_CREDENTIALS']['host'],
								 self.db_cred['MASTER_DB_CREDENTIALS']['user'],
								 self.db_cred['MASTER_DB_CREDENTIALS']['password'], schema)
			cur = db.cursor()
			return db, cur
		except TypeError as err:
			self.error_logger.store_error_log(self.exception_to_string(err))
			print('Error Connection Value')
			return False
		except MySQLdb.OperationalError as err:
			self.error_logger.store_error_log(self.exception_to_string(err))
			print("MySQL Operationial Error:", err)
			return False
		except (MySQLdb.Error, MySQLdb.Warning) as err:
			self.error_logger.store_error_log(self.exception_to_string(err))
			print("MySQL Error:", err)
			return False

	def get_gsm_info(self, gsm_id):
		gsm_dict = {}
		query = "SELECT * FROM gsm_modules where gsm_id = '" + \
			str(gsm_id)+"';"
		gsm_info = self.read_db(query)  # Refactor this
		for gsm_id, gsm_server_id, gsm_name, sim_num, network, port, pwr, rng, module_type in gsm_info:
			gsm_dict['gsm_id'] = gsm_id
			gsm_dict['gsm_server_id'] = gsm_server_id
			gsm_dict['gsm_name'] = gsm_name
			gsm_dict['sim_num'] = sim_num
			gsm_dict['network'] = network
			gsm_dict['port'] = port
			gsm_dict['pwr'] = pwr
			gsm_dict['rng'] = rng
			gsm_dict['module_type'] = module_type
		container = {gsm_id: gsm_dict}
		return container

	def write_csq(self, gsm_id, datetime, csq):
		query = "INSERT INTO gsm_csq_logs VALUES (0, %d, '%s', %d)" % (gsm_id, datetime, csq)
		self.write_to_db(query=query, last_insert_id=False)

	def exception_to_string(self, excp):
		stack = traceback.extract_stack()[:-3] + traceback.extract_tb(excp.__traceback__) 
		pretty = traceback.format_list(stack)
		return ''.join(pretty) + '\n  {} {}'.format(excp.__class__,excp)