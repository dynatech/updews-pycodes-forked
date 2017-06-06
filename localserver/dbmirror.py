import serverdbio as dbio
import MySQLdb
import subprocess
import cfgfileio as cfg

c = cfg.config()

def main():

	# get latest sms_id 
	command ="mysql -u %s -h %s -e 'select max(sms_id) from %s.smsinbox' -p%s" % (c.db["user"],c.dbhost["local"],c.db["name"],c.db["password"])
	p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
	out, err = p.communicate()
	max_sms_id = out.split('\n')[2]
	# max_sms_id = 4104000
	print max_sms_id

	# dump table entries
	f_dump = "/home/dewsl/Documents/sqldumps/mirrordump.sql"
	command = "mysqldump -h %s --skip-add-drop-table --no-create-info -u %s %s smsinbox --where='sms_id > %s' > %s -p%s" % (c.dbhost["gsm"],c.db["user"],c.db["name"],max_sms_id,f_dump,c.db["password"])
	p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
	out, err = p.communicate()
	print err

	# write to local db
	command = "mysql -h %s -u %s %s < %s -p%s" % (c.dbhost["local"],c.db["user"],c.db["name"],f_dump,c.db["password"])
	print command
	p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
	out, err = p.communicate()
	print err

	# delete dump file
	command = "rm %s" % (f_dump)
	p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
	out, err = p.communicate()


if __name__ == "__main__":
    main()