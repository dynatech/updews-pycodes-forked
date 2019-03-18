import pytest
import sys
import pprint
from gsmserver_dewsl3.gsm_modules import ResetException
from gsmserver_dewsl3.gsm_modules import GsmModem

gsm_sms = None
gsm_defaults = None
gsm_reset = None
gsm_modem = None

def setup_module(module):
	global gsm_reset
	global gsm_modem

	gsm_reset = ResetException()
	gsm_modem = GsmModem()

def teardown_module(module):
	pass

def test_categorize_message():
	print("TEST")