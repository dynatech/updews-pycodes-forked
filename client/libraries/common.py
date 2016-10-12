import sys

def whoami():
    return sys._getframe(1).f_code.co_name