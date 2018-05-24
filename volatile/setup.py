import config
import memory
import static
import argparse

def get_arguments():
    parser = argparse.ArgumentParser(description = ("Setup static variables in "
        "memory [-options]"))
    parser.add_argument("-r", "--reset_variables", 
        help="smsinbox table (loggers or users)", action = "store_true")
    
    try:
        args = parser.parse_args()

        return args        
    except IndexError:
        print '>> Error in parsing arguments'
        error = parser.format_help()
        print error
        sys.exit()

def main():

    args = get_arguments()
    config.main()
    static.main(args)
    static.set_static_variable()
    
if __name__ == "__main__":
    main()