import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import config
import static


def get_arguments():
    parser = argparse.ArgumentParser(description = ("Setup static variables in "
        "memory [-options]"))
    parser.add_argument("-r", "--reset_variables", 
        help="smsinbox table (loggers or users)", action = "store_true")
    
    try:
        args = parser.parse_args()

        return args        
    except IndexError:
        print ('>> Error in parsing arguments')
        error = parser.format_help()
        print (error)
        sys.exit()

def main():

    args = get_arguments()
    config.set_cnf("dyna_config.cnf","server_config")
    config.set_cnf("connections.cnf", "DICT_DB_CONNECTIONS")

    # reverse default of reset_variables 
    if args.reset_variables:
        args.reset_variables = False
    else:
        args.reset_variables = True
    static.set_variables_old(args.reset_variables)
    static.set_static_variable()

    
if __name__ == "__main__":
    main()