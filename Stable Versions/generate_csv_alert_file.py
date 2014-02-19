from read_update_proc_plot_data import *
import time as tm

import sys
import ConfigParser

def main():
    GeneratePlots()
    print tm.asctime()
    print "Sleeping..."
    tm.sleep(1200)

if __name__ == '__main__':
    cfg = ConfigParser.ConfigParser()
    cfg.read('node-alerts-config.txt')
    Debug = cfg.getboolean('Misc','Debug')
    if Debug:
        print "Debug Mode"
        main()
    else:
        print "Generating alert file.. "
        while True:
            try:
                main()
            except KeyboardInterrupt:
                print '>> Exiting gracefully.'
            except:
                print tm.asctime()
                print "Unexpected error:", sys.exc_info()[0]
                tm.sleep(5)

