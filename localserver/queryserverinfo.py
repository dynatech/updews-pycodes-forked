import os,time,serial,re,sys
import datetime
from datetime import datetime as dt
from datetime import timedelta as td
import serverdbio as dbio
import mainserver as server
import cfgfileio as cfg
import argparse
import StringIO
import gsmio

def get_latest_query_reportt():
	c = cfg.config()
	querylatestreportoutput = c.fileio.queryoutput
	print querylatestreportoutput
	f = open(querylatestreportoutput,'r')
	filecontent = f.read()
	f.close()

	try:
		return re.search("(?<=Active loggers: )\d{1,2}(?=\W)",filecontent).group(0)
	except:
		return 'ERROR'

def get_runtime_logs(db='local'):
	if db == 'local':
		script1 = 'procfro'
		script2 = 'alert'
		status1 = 'alive'
		status2 = 'checked'
	else:
		script1 = 'globe'
		script2 = 'smart'
		status1 = 'alive'
		status2 = 'alive'
	
	query = """(select * from runtimelog 
		where script_name = '%s' and status = '%s'
		order by timestamp desc limit 1)
		union
		(select * from runtimelog 
		where script_name = '%s' and status = '%s'
		order by timestamp desc limit 1)
		""" % (script1,status1,script2,status2)

	return dbio.query_database(query,'customquery',db)

def get_number_of_reporter(datedt):
	# today = dt.today()

	query = """select numbers from dewslcontacts 
		where nickname = (select nickname from servermonsched 
		where date = '%s')""" % (datedt.strftime("%Y-%m-%d"))

	num = dbio.query_database(query,'customquery')[0][0]

	return num

def get_name_of_staff(number):
	query =  ("select t1.user_id, t2.nickname from user_mobile t1 inner join users t2 on "
		"t1.user_id = t2.user_id where t1.sim_num = '%s';") % (number)

	# print query

	name = dbio.query_database(query,'customquery')[0]

	return name

def send_status_updates(reporter='scheduled'):
	c = cfg.config()
	active_loggers_count = get_latest_query_reportt()

	loclogs = get_runtime_logs('local')
	gsmlogs = get_runtime_logs('gsm')

	status_message = "SERVER STATUS UPDATES\n"
	status_message += "Active loggers: %s\n" % (active_loggers_count)
	status_message += "Last logs for:\n"
	status_message += "Process messages: %s\n" % (loclogs[0][0].strftime("%B %d, %I:%M%p"))
	status_message += "Check alert: %s\n" % (loclogs[0][0].strftime("%B %d, %I:%M%p"))
	status_message += "Globe instance: %s\n" % (gsmlogs[0][0].strftime("%B %d, %I:%M%p"))
	status_message += "Smart instance: %s\n" % (gsmlogs[1][0].strftime("%B %d, %I:%M%p"))

	print dt.today()

	if reporter == 'scheduled':
		reportnumber = get_number_of_reporter(dt.today())
		server.write_outbox_message_to_db(status_message,reportnumber)
	elif int(active_loggers_count) < c.io.active_lgr_limit:
		print ">> Sending alert sms for server"
		server.write_outbox_message_to_db(status_message,c.smsalert.serveralert)

def get_time_of_day_description():
	today = dt.today()

	if today.hour < 12:
		return 'morning'
	elif today.hour>=12 and today.hour < 18:
		return 'afternoon'
	else:
		return 'evening'

def get_number_of_reporter(datedt):
	# today = dt.today()

	query = """select numbers from dewslcontacts 
		where nickname = (select nickname from servermonsched 
		where date = '%s')""" % (datedt.strftime("%Y-%m-%d"))

	num = dbio.query_database(query,'customquery')[0][0]

	return num

def send_server_mon_reminder():
	tomorrow = dt.today()+td(hours=24)
	reportnumber = get_number_of_reporter(tomorrow)

	reminder_message = "Good %s. This is to remind you that you are assigned" % (get_time_of_day_description())
	reminder_message += " for server monitoring duties for tomorrow, %s." % (tomorrow.strftime("%A, %B %d"))
	reminder_message += " As such, you will be receiving regular sms regarding the sever status updates."
	reminder_message += " Thanks."
	server.write_outbox_message_to_db(reminder_message,reportnumber)

def get_shifts(datedt):

	query = """select * from monshiftsched where timestamp < "%s" order by timestamp desc limit 1
	""" % (datedt.strftime("%Y-%m-%d %H:%M:00"))

	return dbio.query_database(query,'customquery')

def get_numbers_from_list(personnel_list):

	query = """select nickname,numbers from dewslcontacts where nickname in %s""" % (str(personnel_list))

	return dbio.query_database(query,'customquery')

def send_event_monitoring_reminder():
	next_shift = dt.today()+td(hours=13)
	shifts = get_shifts(next_shift)
	report_dt = shifts[0][1]

	position = ['iompmt','iompct','oomps','oompmt','oompct']
	position_dict = {}
	for pos,per in zip(position,shifts[0][2:]):
		try:
			position_dict[per.upper().strip()] = pos
		except AttributeError:
			print 'Skiping position'
			continue
	names = tuple([name for name in shifts[0][1:] if name is not None])
	numbers = getNumbersFromList(names)
	numbers_dict = {}
	for nick,num in numbers:
		numbers_dict[nick.upper().strip()] = num

	for key in position_dict:
		reminder_message = "Monitoring shift reminder. Good %s %s, " % (get_time_of_day_description(),key)
		reminder_message += "you are assigned to be the %s " % (position_dict[key].upper())
		reminder_message += "for %s." % (report_dt.strftime("%B %d, %I:%M%p"))
		# print reminder_message

	 	server.write_outbox_message_to_db(reminder_message,numbers_dict[key])

def introduce():
	query = """select nickname,numbers from dewslcontacts
			where grouptags not like "%admin" 
			group by nickname
			"""
	print dbio.query_database(query,'customquery')
	return dbio.query_database(query,'customquery')

def get_non_reporting_sites():
	c = cfg.config()
	two_weeks_ago = (dt.today() - td(days=14)).strftime("%Y-%m-%d")
	query = """ SELECT name FROM `senslopedb`.`site_rain_props` s
		where name not in
		(
		SELECT site_id FROM `senslopedb`.`gndmeas` g
		where timestamp > '%s'
		#where site_id = 'agb'
		group by site_id
		)
		""" % (two_weeks_ago)
		
	non_rep_sites = dbio.query_database(query,'nonreporting')
	accu_sites = []
	for s in non_rep_sites:
		accu_sites.append(s[0])
	print accu_sites
	
	message = "Non reporting sites reminder.\n"
	message += "As of %s, the following sites have no ground data measurement: \n" % (two_weeks_ago)
	message += str(accu_sites)[1:-1]
	message = message.replace("'","")
	print repr(message)
	server.write_outbox_message_to_db(message,c.smsalert.communitynum)

def get_latest_sms_from_column(colname):
	query = """ select timestamp,sms_msg from smsinbox where sms_msg like '%s%s*%s' 
		and sms_msg not like '%spsir%s' order by timestamp desc limit 1 """ % ('%',colname,'%','%','%')
	
	#query = """
	#	select timestamp,sms_msg from senslopedb.smsinbox t1,
	#	(select sim_num from senslopedb.smsinbox
	#	where sms_msg like '%s%s*%s'
	#	order by sms_id desc limit 1
	#	)  t2
	#	where t1.sim_num = t2.sim_num
	#	order by timestamp desc limit 1;
	#""" % ('%',colname,'%')
	
	last_col_msg = dbio.query_database(query,'getlatestcolmsg','gsm')
	print last_col_msg

	tmp_msg = "Latest message from %s\n" % (colname.upper())
	if last_col_msg:
		tmp_msg += 'Timestamp: ' + last_col_msg[0][0].strftime("%Y-%m-%d %H:%M:%S") + '\n'
		tmp_msg += 'Message: ' + last_col_msg[0][1]
	else:
		tmp_msg += 'UNDEFINED'
	return tmp_msg

def get_sim_num_of_column(colname):
	query = """ select sim_num from site_column_sim_nums where name = '%s'; """ % (colname)

	num = dbio.query_database(query,'getsimumofcolumn')

	tmp_msg = "%s: " % (colname.upper())
	if num:
		return tmp_msg + num[0][0]
	else:
		return tmp_msg + 'UNDEFINED'

def get_latest_data_of_node(colname,nid):
	query = """ select * from %s where id = %d and 
	timestamp = (select max(timestamp) from %s where id = %d); """ % (colname,nid,colname,nid)

	print query
	data = dbio.query_database(query,'get_latest_data_of_node')
	tmp_msg = "Latest data from %s %d\n" % (colname.upper(),nid)
	for line in data:
		tmp_msg += line[0].strftime("%Y-%m-%d %H:%M:%S") + ','
		tmp_msg += repr(line[1:]).replace('L','') + '\n'
	
	if len(data) == 0:
		tmp_msg += 'UNDEFINED'
	else:
		print ">> ", data
	
	return tmp_msg

def main():
	func = sys.argv[1] 
	if func == 'sendregularstatusupdates':
		send_status_updates()
	elif func == 'send_server_mon_reminder':
		send_server_mon_reminder()
	elif func == 'send_event_monitoring_reminder':
		send_event_monitoring_reminder()
	elif func == 'introduce':
		introduce()
	elif func == 'sendserveralert':
		send_status_updates('server')
	elif func == 'get_non_reporting_sites':
		get_non_reporting_sites()
	elif func == 'test':
		test()
	else:
		print '>> Wrong argument'

def get_personnel_of_group(groupname):
	query = """ select nickname from dewslcontacts where """

	for g in groupname.split(","):
		query += """grouptags like '%s%s%s' or """ % ('%',g,'%')

	# remove trailing or
	query = re.sub(" or $", "", query)

	return dbio.query_database(query,'customquery')


# if __name__ == "__main__":
# 	main()
def server_messaging(msg):
	print msg.data

	sender = get_name_of_staff(msg.simnum)

	group_tags = msg.data.split(" ")[1]

	messagetosend = msg.data.split(" ",2)[2]

	person_list = get_personnel_of_group(group_tags)

	# print str(person_list)

	person_list = re.findall("(?<=')\w+(?=')",str(person_list))

	# print str(person_list)[1:-1]

	personnel_number_list = get_numbers_from_list('('+str(person_list)[1:-1]+')')

	messagetosend = "From: %s\nTo: %s\n%s" % (sender,group_tags,messagetosend)


	for pnl in personnel_number_list:
		server.write_outbox_message_to_db(messagetosend,pnl[1])

	return True

def process_server_info_request(msg):
	parser = argparse.ArgumentParser(description="Request information from server\n PSIR [-options]")
	parser.add_argument("-s", "--sim_num", help="get sim number of column", action="store_true")
	parser.add_argument("-t", "--latest_ts", help="get timestamp of latest sms", action="store_true")
	parser.add_argument("-d", "--latest_node_data", help="get latest node data", action="store_true")
	
	parser.add_argument("-c", "--col_name", help="column name")
	parser.add_argument("-n", "--node_id", help="node id", type=int)
	# parser.add_argument("-m", "--msg_id", help="message id", type=int)
	
	# check if there is an error in parsing the arguments
	print msg.data
	try:
		args = parser.parse_args(msg.data.lower().split(' ')[1:])
	except:
		print '>> Error in parsing'
		# error_msg = StringIO.StringIO()
		error = parser.format_help().replace("processmessagesfromdb.py","PSRI")
		# error = error_msg.get
		print error
		server.write_outbox_message_to_db(error,msg.simnum)
		return

	if args.sim_num:
		print ">> Sim num request",
		num = get_sim_num_of_column(args.col_name.strip())
		print num
		server.write_outbox_message_to_db(num,msg.simnum)
	
	if args.latest_ts:
		print ">> Latest sms request",
		ts_msg = get_latest_sms_from_column(args.col_name.strip())
		print ts_msg
		server.write_outbox_message_to_db(ts_msg,msg.simnum)

	if args.latest_node_data:
		print ">> Latest data of node", 
		latest_data = get_latest_data_of_node(args.col_name.strip(),args.node_id)
		print latest_data
		server.write_outbox_message_to_db(latest_data,msg.simnum)

	print ">> End of psir"
	return True

		
def test():
    # msg = "-t -clabb -n10"
    # msg = gsmio.sms("1","09176023735","PSIR -T -CLABB -N 10","")
    # msg = gsmio.sms("1","09176023735","SENDGM -GCOMMUNITY -M \"This is a test message GM message from GSM server. Please ignore for now.\"","")
    msg = gsmio.sms("1","09176023735","Sendgm senslope,dynaslope Syntax for sending GM\r\n Sendgm <grouptags> <message>","")    
    server_messaging(msg)

if __name__ == "__main__":
    main()
