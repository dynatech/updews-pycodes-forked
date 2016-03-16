import time
import sys
from db_to_csv import *

def main():

    print time.asctime()
    extract_db2()

    print "Sleep.."
    time.sleep(600)

    #test = raw_input('>> End of Code: Press any key to exit')

   
if __name__ == '__main__':
    #setup(console=["all_receiver2.py"])
    while True:
        try:
            main()
        except KeyboardInterrupt:
            gsm.close()
            print '>> Exiting gracefully.'
        except:
            print time.asctime()
            print "Unexpected error:", sys.exc_info()[0]
            
        
