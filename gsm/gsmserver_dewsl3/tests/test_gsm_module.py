import pytest
import sys
import pprint
from gsm.gsmserver_dewsl3.db_lib import DatabaseConnection
import time

from random import choice
from string import ascii_uppercase

dbcon = None


def setup_module(module):
	global dbcon
	dbcon = test = DatabaseConnection()


def teardown_module(module):
	pass

def test_sending_message_1():
	message = "TEST #1"
	recipients = ['639665346097']
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(
			message=message, recipients=recipient, table='users')
		# print(insert_smsoutbox)
		assert insert_smsoutbox == 0


def test_send_empty_message_2():
	message = ""
	recipients = ['639665346097']
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(
			message=message, recipients=recipient, table='users')
		# print(insert_smsoutbox)
		assert insert_smsoutbox == -1


def test_send_message_with_new_lines_3():
	message = "First\n\nSecond\n\n\nThird\n\nLAST"
	recipients = ['639665346097']
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(
			message=message, recipients=recipient, table='users')
		# print(insert_smsoutbox)
		assert insert_smsoutbox == 0


def test_send_message_with_special_characters_4():
	message = "To type Ñ or ñ"
	recipients = ['639665346097']
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(
			message=message, recipients=recipient, table='users')
		# print(insert_smsoutbox)
		assert insert_smsoutbox == 0


def test_send_max_character_sms_5():
	message = "THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1THIS IS A TEST MESSAGE#1"
	recipients = ['639665346097']
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(
			message=message, recipients=recipient, table='users')
		# print(insert_smsoutbox)
		assert insert_smsoutbox == 0

def test_receive_message_6():
	message = "TEST RECEIVE SMS"
	recipients = ['639175394337']  #GSM SERVER NUMBER FOR TESTING
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
		assert insert_smsoutbox == 0

def test_receive_159_characters_7():
	message = "11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111 159 characters"
	recipients = ['639175394337']  #GSM SERVER NUMBER FOR TESTING
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
		assert insert_smsoutbox == 0

def test_receive_318_characters_8():
	message = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111 318 characters"
	recipients = ['639175394337']  #GSM SERVER NUMBER FOR TESTING
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
		assert insert_smsoutbox == 0

def test_receive_477_characters_9():
	message = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111 477 characters"
	recipients = ['639175394337']  #GSM SERVER NUMBER FOR TESTING
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
		assert insert_smsoutbox == 0

def test_receive_with_special_characters_10():
	message = "To type Ñ or ñ"
	recipients = ['639175394337']  #GSM SERVER NUMBER FOR TESTING
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
		assert insert_smsoutbox == 0

def test_receive_message_with_new_lines_11():
	message = "New\n\nLine\n\n\nTest\n\ning"
	recipients = ['639175394337']  #GSM SERVER NUMBER FOR TESTING
	for recipient in recipients:
		insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
		assert insert_smsoutbox == 0
>>>>>>> 653d9fa5ddef6cd8c839a95d3fb0267685674538

def test_stress_test_sending_and_receiving():
	counter = 0
	recipients = ['639175394337']
	while True:
		message = ''.join(choice(ascii_uppercase) for i in range(155))
		for recipient in recipients:
			insert_smsoutbox = dbcon.write_outbox(message=message, recipients=recipient, table='users')
			assert insert_smsoutbox == 0
			time.sleep(10)
		counter = counter + 1
