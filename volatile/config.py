import configparser
import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import volatile.memory as memory


def set_cnf(file='', static_name=''):
    cnffiletxt = file
    cfile = os.path.dirname(os.path.realpath(__file__)) + '/' + cnffiletxt

    if not os.path.isfile(cfile):
        raise ValueError("File does not exist: %s" % (cfile))

    cnf = configparser.ConfigParser(inline_comment_prefixes=';')
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

            try:
                options[opt] = cnf.getfloat(section, opt)
                continue
            except ValueError:
                pass

            options[opt] = cnf.get(section, opt)
     

        config_dict[section.lower()]= options
    # print config_dict
    memory.set(static_name, config_dict)

    return True
