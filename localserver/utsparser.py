import gsmio
import re
import ast
from datetime import datetime as dt
import serverdbio as dbio

def get_extensometer_id(uts_name):
	query = ("select extensometer_id from extensometers where "
			"extensometer_name = '%s'") % uts_name;

	try:
		x_id = dbio.query_database(query)[0][0]
	except IndexError:
		print ">> Error: no record for (%s) in database" % uts_name
		x_id = 0

	return x_id


def parse_extensometer_uts(msg):
	values = {}

	uts_name = re.search("^[A-Z]{5}(?=\*L\*)",msg.data).group(0)
	values["uts_name"] = uts_name

	x_id = get_extensometer_id(uts_name)
	if x_id == 0:
		return False
	else:
		values['x_id'] = x_id	

	uts_data = re.search("(?<=[A-Z]{5}\*L\*).*(?=\*[0-9]{12})",msg.data).group(0).lower()
	
	for val_pair in uts_data.split(","):
		val_pair_unpacked = val_pair.split(":")
		key = val_pair_unpacked[0]
		val = val_pair_unpacked[1]

		try:
			values[key] = int(val)
		except ValueError:
			try:
				values[key] = float(val)
			except ValueError:
				print ">> Value conversion error %s" % (key)
				return False

	ts = re.search("(?<=\*)[0-9]{12}(?=$)",msg.data).group(0)
	ts = dt.strptime(ts,"%y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
	values["ts"] = ts

	print values 

	query = ("insert into extensometer_uts_data (ts,lag,maximum_val,"
		"maximum_index,extensometer_id,temp_val) values "
		"('%s',%d,%f,%d,%d,%f)") % (values['ts'], values['la'], values['mx'],
		values['mi'], values['x_id'], values['tp'])

	print dbio.commit_to_db(query,'uts')


	return True