import pytest
import sys
import pprint
from gsm.gsmserver_dewsl3.db_lib import DatabaseConnection

dbcon = None

def setup_module(module):
	global dbcon
	dbcon = test = DatabaseConnection()

def teardown_module(module):
	pass

def test_sending_message():
	message = "TEST #1"
	recipients = ['639665346097']
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
		# print(insert_smsoutbox)
		assert insert_smsoutbox == 0

def test_send_empty_message():
	message = ""
	recipients = ['639665346097']
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
		# print(insert_smsoutbox)
		assert insert_smsoutbox == -1

def test_send_message_with_new_lines():
	message = "First\n\nSecond\n\n\nThird\n\nLAST"
	recipients = ['639665346097']
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
		# print(insert_smsoutbox)
		assert insert_smsoutbox == 0

def test_send_message_with_special_characters():
	message = "To type Ñ or ñ"
	recipients = ['639665346097']
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
		# print(insert_smsoutbox)
		assert insert_smsoutbox == 0

def test_send_max_character_sms():
	message = "THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1"
	recipients = ['639665346097']
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
		# print(insert_smsoutbox)
		assert insert_smsoutbox == 0

# def test_receive_empty_message():
# 	message = ""
# 	recipients = ['63175394337']  #GSM SERVER NUMBER FOR TESTING
# 	for recipient in recipients:
# 		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
# 		assert insert_smsoutbox == -1

# def test_receive_with_special_characters():
# 	message = ""
# 	recipients = ['639665346097']  #GSM SERVER NUMBER FOR TESTING
# 	for recipient in recipients:
# 		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
# 		assert insert_smsoutbox == -1

# def test_receive_message_with_new_lines():
# 	message = ""
# 	recipients = ['639665346097']  #GSM SERVER NUMBER FOR TESTING
# 	for recipient in recipients:
# 		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
# 		assert insert_smsoutbox == -1