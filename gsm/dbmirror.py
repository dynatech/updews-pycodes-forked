""" Mirroring Data from dyna to sanbox and sandbox to dyna."""

import serverdbio as dbio
import MySQLdb
import subprocess
import cfgfileio as cfg
import argparse
import memcache

c = cfg.config()
mc = memcache.Client(['127.0.0.1:11211'],debug=0)

def get_arguments():
    """
      **Description:**
        -The function that checks the argument that being sent from main function and returns the
        arguement of the function.
      
      :parameters: N/A
      :returns: **args** (*int*) - Mode of action from running python **1** (*dyna to sandbox*) and **2** (*sandbox to dyna*).
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
      **Description:**
        -The function that process the exporting of data from dyna and importing data to sandbox by 
        loading  the data from XML.
      
      :parameters: N/A
      :returns: N/A
          
    """ 

    # get latest sms_id
    # print c.db["user"],c.dbhost["local"],c.db["name"],c.db["password"]
    sc = mc.get('server_config')
    
    user = sc["db"]["user"]
    password = sc["db"]["password"]
    name = sc["db"]["name"]

    sb_host = sc["hosts"]["sandbox"]
    dyna_host = sc["hosts"]["dynaslope"]
    gsm_host = sc["hosts"]["gsm"]

    
    print "Checking max sms_id in sandbox smsinbox "
    command = ("mysql -u %s -h %s -e 'select max(sms_id) "
        "from %s.smsinbox' -p%s") % (user, sb_host, name, password)
    # print command

    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)
    out, err = p.communicate()
    max_sms_id = out.split('\n')[2]
    # max_sms_id = 4104000
    print "Max sms_id from sandbox smsinbox:", max_sms_id, "done"

    # dump table entries
    print "Dumping tables from gsm host to sandbox dump file ...", 

    f_dump = "/home/dewsl/Documents/sqldumps/mirrordump.sql"
    command = ("mysqldump -h %s --skip-add-drop-table --no-create-info "
        "-u %s %s smsinbox --where='sms_id > %s' > %s -p%s") % (gsm_host, 
            user, name, max_sms_id, f_dump, password)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)
    out, err = p.communicate()
    print 'done'

    # write to local db
    print "Dumping tables from gsm host to sandbox dump file ...", 
    command = "mysql -h %s -u %s %s < %s -p%s" % (sb_host, user, name, f_dump,
        password)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)
    out, err = p.communicate()
    print 'done'

    # delete dump file
    print "Deleting dump file ...", 
    command = "rm %s" % (f_dump)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)
    out, err = p.communicate()
    print 'done'

def get_max_index_from_table(table_name):
    """
      **Description:**
        -The function that get the max index of the smsinbox.
          
          :param table: Name of the table for smsinbox
          :type table: str
          :returns: **max_inbox_id** (*int*) - Index id of the not yet copied 
          data in dyna.
      
    """
    sc = mc.get("server_config")
    smsdb_host = sc["hosts"][sc["resource"]["smsdb"]]
    user = sc["db"]["user"]
    password = sc["db"]["password"]
    name = sc["db"]["name"]

    command = ("mysql -u %s -h %s -e 'select max(inbox_id) from %s.smsinbox_%s "
        "where gsm_id!=1' -p%s") % (user, smsdb_host, name, table_name, 
        password)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)
    out, err = p.communicate()
    return int(out.split('\n')[2])

def get_last_copied_index(table_name):
    """
        **Description:**
            -The function that reads the value of the index inside the user_inbox_index.tmp.
        
        :parameters: N/A
        :returns: **max_index_last_copied** (*int*) - Index id that stored from the user_inbox_index.tmp.
    """
    f_index = open("/home/dyna/logs/%s_inbox_index.tmp" % table_name, 'r')
    max_index_last_copied = int(f_index.readline())
    f_index.close()
    return max_index_last_copied

def import_sql_file_to_dyna(table, max_inbox_id, max_index_last_copied):
    """
        **Description:**
         -The function that process the exporting of data from sanbox and importing data to dyna smsibox2.
         This function also change the value of the index in user_inbox_index.tmp.
        :param table: Name of the table for smsinbox
        :param max_inbox_id: Index id of the not yet copied data in dyna
        :param max_index_last_copied: Index id that stored from the user_inbox_index.tmp
        :type table: str
        :type max_inbox_id: int
        :type max_index_last_copied: int
        :returns: N/A
    """
    print "importing to dyna tables"
    print table

    sc = mc.get("server_config")
    
    smsdb_host = sc["resource"]["smsdb"]
    smsdb2_host = sc["resource"]["smsdb2"]

    host_ip = sc["hosts"][smsdb_host]
    host_ip2 = sc["hosts"][smsdb2_host]
    user = sc["db"]["user"]
    password = sc["db"]["password"]
    name = sc["db"]["name"]

    copy_query = ("SELECT t1.ts_sms as 'timestamp', t2.sim_num, t1.sms_msg, "
        "'UNREAD' as read_status, 'W' AS web_flag FROM smsinbox_%s t1 "
        "inner join (select mobile_id, sim_num from %s_mobile) t2 "
        "on t1.mobile_id = t2.mobile_id where t1.gsm_id !=1 "
        "and t1.inbox_id < %d and t1.inbox_id > %d") % (table, table[:-1], 
            max_inbox_id, max_index_last_copied)

    f_dump = "/home/dyna/logs/sandbox_%s_dump.sql" % (table)

    # export files from the table and dump to a file

    command = ("mysql -e \"%s\" -h%s senslopedb -uroot -p%s --xml >"
            " %s" % (copy_query, host_ip, password, f_dump))
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)
    out, err = p.communicate()
    print command
    print ""
    # # print err

    import_query = ("LOAD XML LOCAL INFILE '%s' INTO TABLE smsinbox2" % (f_dump))
    command = ("mysql -e \"%s\" -h%s senslopedb -uroot -p%s") % (import_query,
        host_ip2, password)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)
    out, err = p.communicate()
    print command

    # write new value in max_index_last_copied
    f_index = open("/home/dyna/logs/%s_inbox_index.tmp" % table, "wb")
    f_index.write(str(max_inbox_id))
    f_index.close()

    # delete dump file
    command = "rm %s" % (f_dump)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, 
        stderr=subprocess.STDOUT)


def sandbox_to_dyna(table_name):
    """
      **Description:**
        -The function that process the mirroring data of sandbox to dyna by comparing
        the max index of  the not yet copied data from dyna and the last copied index
        from dyna.
      
      :parameters: N/A
      :returns: N/A
      
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
        **Description:**
          -The main function that runs the whole dbmirror with the logic of
          checking if the dbmirror must run the dyna to sandbox 
          or the sandbox to dyna for mirroring the data.
         
        :parameters: N/A
        :returns: N/A
        .. note:: To run in terminal **python dbmirror.py -m1** for dyna to sandbox and **python dbmirror.py -m2 -t users/loggers** for sanbox to dyna.
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
