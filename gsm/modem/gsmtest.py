import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import modem

gsm = modem.GsmModem('/dev/xbeeusbport', 9600, 29, 22)
gsm.set_defaults()
print (gsm.reset())