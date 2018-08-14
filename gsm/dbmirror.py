""" Mirroring Data from dyna to sanbox and sandbox to dyna."""

import MySQLdb
import subprocess
import argparse
import memcache
import sys
import volatile.memory as mem

def get_arguments():
    """
    - The get arguments is a function that checks the argument that being sent from main function and returns the
      arguement of the function.

    Returns: 
        int: Outputs mode of action from running python **1** (*dyna to sandbox*) and **2** (*sandbox to dyna*).
    
    .. note:: For mode 2 it also return **users or loggers** for sanbox to dyna.
    """
    parser = argparse.ArgumentParser(description = ("copy items from different"
     "inboxes [-options]"))
    parser.add_argument("-m", "--mode", type = int, help="mode")
    parser.add_argument("-t", "--execute", help="execute")

    try:
        args = parser.parse_args()

        # if args.status == None:
        #     args.status = 0
        # if args.messagelimit == None:
        #     args.messagelimit = 200
        return args
    except IndexError:
        print '>> Error in parsing arguments'
        error = parser.format_help()
        print error
        sys.exit()

def dyna_to_sandbox():
    """
    - The process of dyna to sandbox is a function that process the exporting of data from dyna and importing data to sandbox by 
      loading  the data from XML.
            
    """ 

    # get latest sms_id
    # print c.db["user"],c.dbhost["local"],c.db["name"],c.db["password"]
    sc = mem.server_config()
    
    user = sc["db"]["user"]
    password = sc["db"]["password"]
    name = sc["db"]["name"]

    sb_host = sc["hosts"]["sandbox"]
    dyna_host = sc["hosts"]["dynaslope"]
    gsm_host = sc["hosts"]["gsm"]
    sqldumpsdir = sc["fileio"]["sqldumpsdir"]

    
    print "Checking max sms_id in sandbox smsinbox "
    command = ("mysql -u %s -h %s -e 'select max(sms_id) "
        "from %s.smsinbox' -p%s") % (user, sb_host, name, password)
    # print command

    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)
    out, err = p.communicate()
    try:
        max_sms_id = out.split('\n')[2]
    except IndexError:
        print "Index Error"
        print out, err
        sys.exit()
    # max_sms_id = 4104000
    print "Max sms_id from sandbox smsinbox:", max_sms_id
    print  "done\n"

    # dump table entries
    print "Dumping tables from gsm host to sandbox dump file ...", 

    f_dump = sqldumpsdir + "mirrordump.sql"
    command = ("mysqldump -h %s --skip-add-drop-table --no-create-info --single-transaction "
        "-u %s %s smsinbox --where='sms_id > %s' > %s ") % (gsm_host, 
            user, name, max_sms_id, f_dump)
    print command
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)
    out, err = p.communicate()
    if out or err:
        print ">> Error on dyna mysql > dump"
        print out, err
    else:
        print ">> No errors"
    print 'done\n'

    # write to local db
    print "Dumping tables from gsm host to sandbox dump file ...", 
    command = "mysql -h %s -u %s %s < %s" % (sb_host, user, name, f_dump)
    print command
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)
    out, err = p.communicate()
    if out or err:
        print ">> Error on sandbox mysql < dump"
        print out, err
    else:
        print ">> No errors"
    print 'done\n'

    # delete dump file
    print "Deleting dump file ..."
    command = "rm %s" % (f_dump)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)
    out, err = p.communicate()
    print 'done\n'

def get_max_index_from_table(table_name):
    """
    - The process getting of  max index from table is a function that get the max index of the smsinbox.
          
      :param table: Name of the table for smsinbox
      :type table: str

    Returns: 
         int: Index id of the not yet copied data in dyna.
      
    """
    sc = mem.server_config()
    smsdb_host = sc["hosts"][sc["resource"]["smsdb"]]
    user = sc["db"]["user"]
    password = sc["db"]["password"]
    name = sc["db"]["smsdb_name"]

    command = ("mysql -u %s -h %s -e 'select max(inbox_id) from %s.smsinbox_%s "
        "where gsm_id!=1' -p%s") % (user, smsdb_host, name, table_name, 
        password)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)
    out, err = p.communicate()
    return int(out.split('\n')[2])

def get_last_copied_index(table_name):
    """
    - The process of getting last copied index is a  function that reads the 
      value of the index inside the user_inbox_index.tmp.
        
    Returns: 
        int: Outputs index id that stored from the user_inbox_index.tmp.
    """
    sc = mem.server_config()
    logsdir = sc["fileio"]["logsdir"]
    tmpfile = logsdir + ("%s_inbox_index.tmp" % table_name)
    f_index = open(tmpfile, 'r')
    max_index_last_copied = int(f_index.readline())
    f_index.close()
    return max_index_last_copied

def import_sql_file_to_dyna(table, max_inbox_id, max_index_last_copied):
    """
    - The process of importing sql file to dyna is a function that process the exporting of data from sanbox and importing data to dyna smsibox2.
    This function also change the value of the index in user_inbox_index.tmp.

    :param table: Name of the table for smsinbox
    :param max_inbox_id: Index id of the not yet copied data in dyna
    :param max_index_last_copied: Index id that stored from the user_inbox_index.tmp
    :type table: str
    :type max_inbox_id: int
    :type max_index_last_copied: int

    """
    print "importing to dyna tables"
    print table

    sc = mem.server_config()
    
    smsdb_host = sc["resource"]["smsdb"]
    smsdb2_host = sc["resource"]["smsdb2"]

    host_ip = sc["hosts"][smsdb_host]
    host_ip2 = sc["hosts"][smsdb2_host]
    user = sc["db"]["user"]
    password = sc["db"]["password"]
    smsdb_name = sc["db"]["smsdb_name"]
    name = sc["db"]["name"]
    logsdir = sc["fileio"]["logsdir"]
    sqldumpsdir = sc["fileio"]["sqldumpsdir"]

    copy_query = ("SELECT t1.ts_sms as 'timestamp', t2.sim_num, t1.sms_msg, "
        "'UNREAD' as read_status, 'W' AS web_flag FROM smsinbox_%s t1 "
        "inner join (select mobile_id, sim_num from %s_mobile) t2 "
        "on t1.mobile_id = t2.mobile_id where t1.gsm_id !=1 "
        "and t1.inbox_id < %d and t1.inbox_id > %d") % (table, table[:-1], 
            max_inbox_id, max_index_last_copied)

    f_dump = logsdir + ("sandbox_%s_dump.sql" % (table))

    # export files from the table and dump to a file

    command = ("mysql -e \"%s\" -h%s %s -upysys_local -p%s --xml >"
            " %s" % (copy_query, host_ip, smsdb_name, password, f_dump))
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)
    out, err = p.communicate()
    print command
    print ""
    # # print err

    import_query = ("LOAD XML LOCAL INFILE '%s' INTO TABLE smsinbox2" % (f_dump))
    command = ("mysql -e \"%s\" -h%s senslopedb -upysys_local -p%s") % (import_query,
        host_ip2, password)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)
    out, err = p.communicate()
    print command

    # write new value in max_index_last_copied
    tmpfile = logsdir + ("%s_inbox_index.tmp" % table)
    f_index = open(tmpfile, "wb")
    f_index.write(str(max_inbox_id))
    f_index.close()

    # delete dump file
    command = "rm %s" % (f_dump)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)


def sandbox_to_dyna(table_name):
    """
    - The process of sandbox to dyna is a function that process the mirroring data of sandbox to dyna by comparing
    the max index of  the not yet copied data from dyna and the last copied index
    from dyna.
    """
    # get index of items not yet copied to dyna
    print "sandbox -> dyna"

    table = table_name
    max_inbox_id = get_max_index_from_table(table)
    print "max index from table:", max_inbox_id

    # check if this index is already copied
    max_index_last_copied = get_last_copied_index(table)
    print "max index copied:", max_index_last_copied

    if max_inbox_id > max_index_last_copied:
        import_sql_file_to_dyna(table, max_inbox_id, max_index_last_copied)
    else:
        print "smsinbox2 up to date"

def main():
    """
    - The main is a function that runs the whole dbmirror with the logic of
      checking if the dbmirror must run the dyna to sandbox 
      or the sandbox to dyna for mirroring the data.

    .. todo::
        1. To run the script **open** a terminal or bash 
        2. Set your terminal/bash path to **/centraserver/gsm/**
        3. Type inside the terminal/bash  **python dbmirror.py -m1** for dyna
           to sandbox and **python dbmirror.py -m2 -t users/loggers** for sanbox 
           to dyna.
        4. Click Enter
       
    """

    args = get_arguments()

    if args.mode == 1:
        dyna_to_sandbox()
    elif args.mode == 2:
        sandbox_to_dyna(args.execute)
    elif args.mode is None:
        print ">> Error: No mode given"
    else:
        print ">> Error: mode not available (%d)" % (args.mode)


if __name__ == "__main__":
    main()
