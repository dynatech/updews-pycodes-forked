import memory
from datetime import datetime as dt
import os
import ConfigParser


def set_cnf(file='', static_name=''):
    cnffiletxt = file
    cfile = os.path.dirname(os.path.realpath(__file__)) + '/' + cnffiletxt
    cnf = ConfigParser.ConfigParser()
    cnf.read(cfile)

    config_dict = dict()
    for section in cnf.sections():
        options = dict()
        for opt in cnf.options(section):

            try:
                options[opt] = cnf.getboolean(section, opt)
                continue
            except ValueError:
                pass

            try:
                options[opt] = cnf.getint(section, opt)
                continue
            except ValueError:
                pass

            options[opt] = cnf.get(section, opt)
     

        config_dict[section.lower()]= options
    # print config_dict
    memory.set(static_name, config_dict)
