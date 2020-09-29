import pytest
import sys
import pprint
from gsm.gsmserver_dewsl3.db_lib import DatabaseConnection
import time
from datetime import datetime as dt
from datetime import timedelta as td
import MySQLdb

from random import choice
from string import ascii_uppercase

def teardown_module(module):
	pass

def insert_inbox(ts_written, message):
	dbcon = MySQLdb.connect('192.168.150.253',
							'root',
							'senslope', 'comms_db')
	cur = dbcon.cursor()
	query = ('INSERT INTO smsoutbox_users VALUES (0, "%s", "central", "%s");commit;') % (ts_written, message)
	print(query)
	exec = cur.execute(query)
	query = 'SELECT outbox_id from smsoutbox_users order by outbox_id desc limit 1;'
	cur.execute(query)
	id = str(cur.fetchone()[0])
	query = 'INSERT INTO smsoutbox_user_status VALUES (0, "%s", "25",NULL, 0, 20);commit;' % id
	result = cur.execute(query)
	dbcon.close()
	return result

def test_sending_message_1():
	message = 'pytest # 1'
	ts_written = dt.today().strftime("%Y-%m-%d %H:%M:%S")
	result = insert_inbox(ts_written, message)
	assert result == 1

def test_send_159_characters():
	message = 'lorem Ipsum is simply dummy text of the printing and typesetting ' \
		'industry. Lorem Ipsum has been the industry\'s standard dummy text ever since the 1500s, when a'
	ts_written = dt.today().strftime("%Y-%m-%d %H:%M:%S")
	result = insert_inbox(ts_written, message)
	assert result == 1

def test_send_320_characters():
	message =  'lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry\'s standard ' \
		'dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. ' \
		'It has survived not only five centuries, but also the leap into electronic'
	ts_written = dt.today().strftime("%Y-%m-%d %H:%M:%S")
	result = insert_inbox(ts_written, message)
	assert result == 1


def test_send_640_characters():
	message = 'lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry \'s ' \
	'standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. ' \
	'It has survived not only five centuries, but also the leap into electronic typesetting, ' \
	'remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets ' \
	'containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.'
	ts_written = dt.today().strftime("%Y-%m-%d %H:%M:%S")
	result = insert_inbox(ts_written, message)
	assert result == 1


def test_send_1000_characters():
	message = ''.join(choice(ascii_uppercase) for i in range(1000))
	ts_written = dt.today().strftime("%Y-%m-%d %H:%M:%S")
	result = insert_inbox(ts_written, message)
	assert result == 1

# def test_receive_message_6():
# 	message = "TEST RECEIVE SMS"
# 	recipients = ['639056676763']  #GSM SERVER NUMBER FOR TESTING
# 	for recipient in recipients:
# 		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
# 		assert insert_smsoutbox == 0

# def test_receive_159_characters_7():
# 	message = "11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111 159 characters"
# 	recipients = ['639056676763']  #GSM SERVER NUMBER FOR TESTING
# 	for recipient in recipients:
# 		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
# 		assert insert_smsoutbox == 0

# def test_receive_318_characters_8():
# 	message = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111 318 characters"
# 	recipients = ['639056676763']  #GSM SERVER NUMBER FOR TESTING
# 	for recipient in recipients:
# 		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
# 		assert insert_smsoutbox == 0

# def test_receive_477_characters_9():
# 	message = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111 477 characters"
# 	recipients = ['639056676763']  #GSM SERVER NUMBER FOR TESTING
# 	for recipient in recipients:
# 		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
# 		assert insert_smsoutbox == 0

# def test_receive_with_special_characters_10():
# 	message = "To type Ñ or ñ"
# 	recipients = ['639056676763']  #GSM SERVER NUMBER FOR TESTING
# 	for recipient in recipients:
# 		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
# 		assert insert_smsoutbox == 0

# def test_receive_message_with_new_lines_11():
# 	message = "New\n\nLine\n\n\nTest\n\ning"
# 	recipients = ['639056676763']  #GSM SERVER NUMBER FOR TESTING
# 	for recipient in recipients:
# 		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
# 		assert insert_smsoutbox == 0

# def test_stress_test_sending_and_receiving():
# 	counter = 0
# 	recipients = ['639056676763']
# 	while True:
# 		message = ''.join(choice(ascii_uppercase) for i in range(500))
# 		for recipient in recipients:
# 			insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
# 			assert insert_smsoutbox == 0
# 			time.sleep(10)
# 		counter = counter + 1
