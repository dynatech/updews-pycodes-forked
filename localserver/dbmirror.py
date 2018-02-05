import serverdbio as dbio
import MySQLdb
import subprocess
import cfgfileio as cfg
import argparse

c = cfg.config()

def get_arguments():
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

        # get latest sms_id
        print c.db["user"],c.dbhost["local"],c.db["name"],c.db["password"]
        command ="mysql -u %s -h %s -e 'select max(sms_id) from %s.smsinbox' -p%s" % (c.db["user"],c.dbhost["local"],c.db["name"],c.db["password"])
        print command
        p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        out, err = p.communicate()
        max_sms_id = out.split('\n')[2]
        # max_sms_id = 4104000
        print "Max sms_id from sandbox smsinbox:", max_sms_id

        # dump table entries
        f_dump = "/home/dewsl/Documents/sqldumps/mirrordump.sql"
        command = "mysqldump -h %s --skip-add-drop-table --no-create-info -u %s %s smsinbox --where='sms_id > %s' > %s -p%s" % (c.dbhost["gsm"],c.db["user"],c.db["name"],max_sms_id,f_dump,c.db["password"])
        print command
        p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        out, err = p.communicate()
        print out, err

        # write to local db
        command = "mysql -h %s -u %s %s < %s -p%s" % (c.dbhost["local"],c.db["user"],c.db["name"],f_dump,c.db["password"])
        print command
        p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        out, err = p.communicate()
        print out, err

        # delete dump file
        command = "rm %s" % (f_dump)
        p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        out, err = p.communicate()

def get_max_index_from_table(table_name):
        command ="mysql -u %s -h %s -e 'select max(inbox_id) from %s.smsinbox_%s where gsm_id!=1' -p%s" % (c.db["user"],c.dbhost["local"],c.db["name"],table_name,c.db["password"])
        p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        out, err = p.communicate()
        max_inbox_id = out.split('\n')[2]
        # max_sms_id = 4104000
        return max_inbox_id

def get_last_copied_index(table_name):
        f_index = open("/home/dewsl/scriptlogs/" + "%s_inbox_index.tmp" % table_name)
        max_index_last_copied = int(f_index.readline())
        f_index.close()
        return max_index_last_copied

def import_sql_file_to_dyna(table,max_inbox_id,max_index_last_copied):
        print "importing to dyna tables"
        print table
        copy_query = ("SELECT t1.ts_sms as 'timestamp', t2.sim_num, t1.sms_msg, 'UNREAD' "
                "as read_status, 'W' AS web_flag FROM smsinbox_%s t1 inner join "
                "(select mobile_id, sim_num from user_mobile) t2 "
                "on t1.mobile_id = t2.mobile_id where t1.gsm_id !=1 and inbox_id <= %s and inbox_id > %s" % (table,max_inbox_id,max_index_last_copied))

        f_dump = "/home/dewsl/Documents/sqldumps/sandbox_%s_dump.sql" % (table)

        # export files from the table and dump to a file
        command = ("mysql -e \"%s\" -h127.0.0.1 senslopedb -uroot -p%s --xml >"
                " %s" % (copy_query,c.db["password"],f_dump))
        print copy_query

        p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        out, err = p.communicate()
        print command
        print ""
        # # print err

        # command = "mysql -h %s -u %s %s < %s -p%s" % (c.dbhost["local"],c.db["user"],c.db["name"],f_dump,c.db["password"])
        import_query = ("LOAD XML LOCAL INFILE '%s' INTO TABLE smsinbox2" % (f_dump))
        command = ("mysql -e \"%s\" -h127.0.0.1 senslopedb -uroot -p%s" % (import_query,c.db["password"]))
        p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        out, err = p.communicate()
        print command

        # # delete dump file
        # command = "rm %s" % (f_dump)
        # p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        # out, err = p.communicate()

        # write new value in max_index_last_copied
        f_index = open("/home/dewsl/scriptlogs/" +table +"_inbox_index.tmp","wb")
        f_index.write(max_inbox_id);
        f_index.close()


def sandbox_to_dyna(table_name):
        # get index of items not yet copied to dyna
        print "sandbox -> dyna"

        table = table_name
        max_inbox_id = get_max_index_from_table(table)
        print "max index from table:", max_inbox_id

        # check if this index is already copied
        max_index_last_copied = get_last_copied_index(table)
        print "max index copied:", max_index_last_copied

        if max_inbox_id > max_index_last_copied:
                import_sql_file_to_dyna(table,max_inbox_id,max_index_last_copied)

def main():

        args = get_arguments()

        if args.mode == 1:
                dyna_to_sandbox()
        elif args.mode == 2:
                sandbox_to_dyna(args.execute)

        else:
                print ">> Error: mode not available (%d)" % (args.mode)


if __name__ == "__main__":
    main()
