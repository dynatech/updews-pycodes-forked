import MySQLdb
import configparser
from datetime import datetime as dt
from datetime import timedelta as td
from pprint import pprint
import sys

class DatabaseCredentials:
	def __new__(self):
		config = configparser.ConfigParser()
		config.read('/home/pi/updews-pycodes/gsm/gsmserver_dewsl3/utils/config.cnf')
		config["CBEWSL_DB_CREDENTIALS"]
		return config


class DatabaseConnection:

	db_cred = None

	def __init__(self):
		self.db_cred = DatabaseCredentials()

	def get_all_outbox_sms_from_db(self, table, send_status, gsm_id):
		if not table:
			raise ValueError("No table definition")

		while True:
			try:
				db, cur = self.db_connect(
					self.db_cred['CBEWSL_DB_CREDENTIALS']['db_comms'])
				query = ("select t1.stat_id,t1.mobile_id,t1.gsm_id,t1.outbox_id,"
						 "t2.sms_msg from "
						 "smsoutbox_%s_status as t1 "
						 "inner join (select * from smsoutbox_%s) as t2 "
						 "on t1.outbox_id = t2.outbox_id "
						 "where t1.send_status < %d "
						 "and t1.send_status >= 0 and t1.send_status < 6 "
						 "and t1.gsm_id = %d LIMIT 100") % (table[:-1], table, send_status, gsm_id)

				a = cur.execute(query)
				out = []
				if a:
					out = cur.fetchall()
					db.close()
				return out

			except MySQLdb.OperationalError:
				print('10.')
				time.sleep(20)

	def get_all_logger_mobile(self, sim_num):
		try:
			db, cur = self.db_connect(
				self.db_cred['CBEWSL_DB_CREDENTIALS']['db_comms'])
			query = ("SELECT * FROM (SELECT "
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
						"AND t1.sim_num IS NOT NULL) as logger_mobile WHERE sim_num like '%"+sim_num[:10]+"%'")

			a = cur.execute(query)
			out = []
			if a:
				out = cur.fetchall()
				db.close()
			return out

		except MySQLdb.OperationalError:
			time.sleep(20)

	def get_all_user_mobile(self, sim_num, mobile_id_flag=False):
		try:
			db, cur = self.db_connect(
				self.db_cred['CBEWSL_DB_CREDENTIALS']['db_comms'])
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

		except MySQLdb.OperationalError as mysqle:
			print("MySQLdb OP Error:", mysqle)
			time.sleep(20)

	def write_inbox(self, msglist='', gsm_info=''):
		if not msglist:
			raise ValueError("No msglist definition")

		if not gsm_info:
			raise ValueError("No gsm_info definition")

		ts_stored = dt.today().strftime("%Y-%m-%d %H:%M:%S")

		gsm_id = gsm_info['id']


		sms_id_ok = []
		sms_id_unk = []
		ts_sms = 0
		ltr_mobile_id = 0

		for msg in msglist:
			ts_sms = msg.dt
			sms_msg = msg.data
			read_status = 0

			query_loggers = self.write_logger_sms(msg, gsm_id, ts_sms, ts_stored, sms_msg)
			if (query_loggers == -1):
				query_users = self.write_users_sms(msg, gsm_id, ts_sms, ts_stored, sms_msg)
		

	def write_logger_sms(self, msg, gsm_id, ts_sms, ts_stored, sms_msg):
		loggers_count = 0
		query_loggers = ("insert into smsinbox_loggers (ts_sms, ts_stored, mobile_id, "
			"sms_msg,read_status,gsm_id) values ")
		logger_mobile_sim_nums = self.get_all_logger_mobile(msg.simnum[-10:])
		if len(logger_mobile_sim_nums) != 0:
			logger_mobile_sim_nums = {sim_num: mobile_id for (mobile_id, sim_num,
									gsm_id) in logger_mobile_sim_nums}
			if msg.simnum in logger_mobile_sim_nums.keys():
				query_loggers += "('%s','%s',%d,'%s',%d,%d)," % (ts_sms, ts_stored,
					logger_mobile_sim_nums[msg.simnum], sms_msg, 0, gsm_id)
				ltr_mobile_id= logger_mobile_sim_nums[msg.simnum]
				loggers_count += 1

			query_loggers = query_loggers[:-1]
			print(">> Data logger SMS...")
			result = self.write_to_db(query=query_loggers)
			return result
		else:
			return -1

	def write_users_sms(self, msg, gsm_id, ts_sms, ts_stored, sms_msg):
		users_count = 0
		query_users = ("insert into smsinbox_users (ts_sms, ts_stored, mobile_id, "
		   "sms_msg,gsm_id) values ")
		user_mobile_sim_nums = self.get_all_user_mobile(msg.simnum[:10])
		if len(user_mobile_sim_nums) != 0:
			user_mobile_sim_nums = {sim_num: mobile_id for (mobile_id, sim_num,
									gsm_id) in user_mobile_sim_nums}
			if msg.simnum in user_mobile_sim_nums.keys():
				query_users += "('%s','%s',%d,'%s',%d)," % (ts_sms, ts_stored,
						   user_mobile_sim_nums[msg.simnum], sms_msg, gsm_id)
				users_count += 1
			
			query_users = query_users[:-1]
			print(">> Users SMS...")
			result = self.write_to_db(query=query_users)
			return result
		else:
			return -1

	def write_to_db(self, query, last_insert_id=False):
		ret_val = None
		db, cur = self.db_connect(
			self.db_cred['CBEWSL_DB_CREDENTIALS']['db_comms'])

		try:
			a = cur.execute(query)
			db.commit()
			if last_insert_id:
				b = cur.execute('select last_insert_id()')
				b = str(cur.fetchone()[0]) 
				ret_val = b
			else:
				ret_val = a

		except IndexError:
			print("IndexError on ")
			print(str(inspect.stack()[1][3]))
		except (MySQLdb.Error, MySQLdb.Warning) as e:
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
				self.db_cred['CBEWSL_DB_CREDENTIALS']['db_comms'])
			a = cur.execute(query)
			out = []
			if a:
				out = cur.fetchall()
				db.close()
			return out

		except MySQLdb.OperationalError as mysqle:
			print("MySQLdb OP Error:", mysqle)
			time.sleep(20)

	def db_connect(self, schema):
		try:
			db = MySQLdb.connect(self.db_cred['CBEWSL_DB_CREDENTIALS']['host'],
								 self.db_cred['CBEWSL_DB_CREDENTIALS']['user'],
								 self.db_cred['CBEWSL_DB_CREDENTIALS']['password'], schema)
			cur = db.cursor()
			return db, cur
		except TypeError:
			print('Error Connection Value')
			return False
		except MySQLdb.OperationalError as mysqle:
			print("MySQL Operationial Error:", mysqle)
			return False
		except (MySQLdb.Error, MySQLdb.Warning) as e:
			print("MySQL Error:", e)
			return False

	def update_sent_status(self, table='', status_list='', resource="sms_data"):
		if not table:
			raise ValueError("No table definition")

		if not status_list:
			raise ValueError("No status list definition")

		query = ("insert into smsoutbox_%s_status (stat_id,send_status,ts_sent,"
				 "outbox_id,gsm_id,mobile_id) values ") % (table[:-1])

		for stat_id, send_status, ts_sent, outbox_id, gsm_id, mobile_id in status_list:
			query += "(%d,%d,'%s',%d,%d,%d)," % (stat_id, send_status, ts_sent,
												 outbox_id, gsm_id, mobile_id)

		query = query[:-1]
		query += (" on duplicate key update stat_id=values(stat_id), "
				  "send_status=send_status+values(send_status),ts_sent=values(ts_sent)")
		self.write_to_db(query=query, last_insert_id=False)

	def get_gsm_info(self, gsm_id):
		gsm_dict = {}
		query = "SELECT * FROM comms_db.gsm_modules where gsm_id = '" + \
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

	def write_outbox(self, message=None, recipients=None, table=None):

		tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")

		if not message:
			print("No message specified for sending, skipping...")
			return -1

		if not recipients:
			print("No recipients specified for sending, skipping...")
			return -1

		recipients = self.get_all_user_mobile(recipients[:10])
		
		query = ("insert into smsoutbox_%s (ts_written,sms_msg) VALUES "
			"('%s','%s')") % (table,tsw,message)
		outbox_id = self.write_to_db(query=query, last_insert_id=True)

		query = ("INSERT INTO smsoutbox_%s_status (outbox_id,mobile_id,gsm_id)"
				" VALUES ") % (table[:-1])


		for recipient in recipients:
			tsw = dt.today().strftime("%Y-%m-%d %H:%M:%S")
			try:
				query += "(%s, %s, %s)," % (outbox_id, recipient[0], recipient[2])
			except KeyError:
				print (">> Error: Possible key error for", r)
				continue
		query = query[:-1]
		print(query)
		self.write_to_db(query=query, last_insert_id=False)
		return 0

	def get_inbox(self, host='local',read_status=0,table='loggers',limit=200,
	resource="sms_data"):
		db, cur = dbio.connect(host=host, resource=resource)

		if table in ['loggers','users']:
			tbl_contacts = '%s_mobile' % table[:-1]
		else:
			raise ValueError('Error: unknown table', table)
		
		while True:
			try:
				query = ("select inbox_id,ts_sms,sim_num,sms_msg from "
					"(select inbox_id,ts_sms,mobile_id,sms_msg from smsinbox_%s "
					"where read_status = %d order by inbox_id desc limit %d) as t1 "
					"inner join (select mobile_id, sim_num from %s) as t2 "
					"on t1.mobile_id = t2.mobile_id ") % (table, read_status, limit,
					tbl_contacts)

				a = cur.execute(query)
				out = []
				if a:
					out = cur.fetchall()
				return out

			except MySQLdb.OperationalError:
				print ('9.',)
				time.sleep(20)

	def write_csq(self, gsm_id, datetime, csq):
		query = "INSERT INTO gsm_csq_logs VALUES (0, %d, '%s', %d)" % (gsm_id, datetime, csq)
		self.write_to_db(query=query, last_insert_id=False)