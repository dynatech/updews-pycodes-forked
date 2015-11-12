from senslopeServer import *
import time, sys

def main():
    RunSenslopeServer("SMART")

if __name__ == '__main__':
##    main()
    while True:
        try:
            main()
        except KeyboardInterrupt:
            gsm.close()
            print '>> Exiting gracefully.'
            break
        except IndexError:
            gsm.close()
            print time.asctime()
            print "Unexpected error:", sys.exc_info()[0]
            time.sleep(10)



