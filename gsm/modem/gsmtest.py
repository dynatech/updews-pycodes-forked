import modem

gsm = modem.GsmModem('/dev/xbeeusbport', 9600, 29, 22)
gsm.set_defaults()
print gsm.reset()