from senslopeServer import *
import time, sys, gsmSerialio

debug = False

def main():
    network = sys.argv[1].upper()

    if network not in ['GLOBE','SMART']:
        print ">> Error in network selection", network
    
    RunSenslopeServer(network)

if __name__ == '__main__':
##    main()

    while True:
        try:
	    #gsmSerialio.resetGsm()
            main()
        except KeyboardInterrupt:
            gsm.close()
            print '>> Exiting gracefully.'
            break
	except gsmSerialio.CustomGSMResetException:
	    print "> Resetting system because of GSM failure"
	    continue
        # except IndexError:
            # gsm.close()
            # print time.asctime()
            # print "Unexpected error:", sys.exc_info()[0]
            # time.sleep(10)

