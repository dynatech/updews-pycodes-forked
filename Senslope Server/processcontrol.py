from crontab import CronTab
import argparse

def main():

	dyna_cron = CronTab(user='dynaslope')

	parser = argparse.ArgumentParser(description="Control crontab items\n PC [-options]")
	parser.add_argument("-ep", "--enable_procmessages", help="enable processmessages scripts", action="store_true")
	parser.add_argument("-dp", "--disable_procmessages", help="disable processmessages scripts", action="store_true")

	args = parser.parse_args()

	if args.enable_procmessages:
		for j in dyna_cron.find_command('processmessages'):
			j.enable()
		dyna_cron.write()
		print 'Process messages enabled'
	elif args.disable_procmessages:
		for j in dyna_cron.find_command('processmessages'):
			j.enable(False)
		dyna_cron.write()
		print 'Process messages disabled'
	

if __name__ == "__main__":
    main()