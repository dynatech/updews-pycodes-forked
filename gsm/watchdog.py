import sys
import os
import subprocess
import datetime
import volatile.memory as mem

class GsmProcess:
    def __init__(self, pattern_str, cmd_str):
        self.pattern_str = pattern_str
        self.cmd_str = cmd_str

def execute_cmd(cmd, wait_for_out = True):
    my_env = os.environ.copy()
    my_env["PATH"] = "/home/pi/centralserver/:" + my_env["PATH"]
    my_env["PYTHONPATH"] = "/home/pi/centralserver/"
    my_env["USER"] = 'pi'
    my_env["USERNAME"] = 'pi'
    p = subprocess.Popen(cmd, stdout = subprocess.PIPE, 
        shell = True, stderr = subprocess.STDOUT,
        env = my_env
        )

    if wait_for_out:
        out, err = p.communicate()
        print "out: %s, err: %s" % (out, err)
        return out, err
    else:
        return

def count_processes(proc):
    cmd_line = "ps ax | grep \"%s\" -c" % (proc.pattern_str)
    out, err = execute_cmd(cmd_line)
    return int(out)

def main():
    print "Time: %s" % (datetime.datetime.now().strftime("%X"))

    try:
        server_id = mem.get("server_config")["gsmio"]["server_id"]
    except KeyError:
        raise ValueError(">> Unable to deteremine server_id from memory")

    DF_GSM_MODULES = mem.get("df_gsm_modules")

    df_gsm_modules_in_server = DF_GSM_MODULES[DF_GSM_MODULES["gsm_server_id"] == server_id]
    gsm_id_list = list(df_gsm_modules_in_server["gsm_id"])

    SCREEN_PATH = "/usr/bin/screen" 
    PYTHON_PATH = "/usr/bin/python"
    SERVER_PATH = "/home/pi/centralserver/gsm/gsmserver.py"

    process_list = []
    for gsm_id in gsm_id_list:
        string_to_match_in_grep = "gsmserver.py -g%d" % gsm_id
        command_to_run = "%s -S g%d -d -m %s %s -g%d" % (SCREEN_PATH, gsm_id,
            PYTHON_PATH, SERVER_PATH, gsm_id)
        proc = GsmProcess(string_to_match_in_grep, command_to_run)
        process_list.append(proc)

    for proc in process_list:
        proc_count = count_processes(proc)
        if proc_count <= 3:
            print "Execute: %s" % (proc.cmd_str)
            execute_cmd(proc.cmd_str, False)
        else:
            print "Script ok:", proc.pattern_str

if __name__ == "__main__":
    main()
