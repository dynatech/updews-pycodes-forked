import sys
import os
import dynadb.db as dynadb
import memcache


def get_handle(print_out = False):
	if print_out:
		print "Connecting to memcache client ...",
	mc = memcache.Client(['127.0.0.1:11211'],debug=0)
	if print_out:
		print "done"
	return mc

def query_static:
	query = 'Select * from static_variables'
	variables = dynadb.read(query = query, 
		identifier = 'Get all static_variables')
	print variables
def set_static(name,data):
	mc = get_handle()
	return mc.set(name,data)

def get_static(name):
	mc = get_handle()
	return mc.get(name)
def data_formatter(id,data):

def main():
    
    # args = get_arguments()

    # if not args.bypasslock:
    #     lockscript.get_lock('smsparser %s' % args.table)

    # # dbio.create_table("runtimelog","runtime")
    # # logRuntimeStatus("procfromdb","startup")

    # print 'SMS Parser'

    # print args.dbhost, args.table, args.status, args.messagelimit
    # allmsgs = smstables.get_inbox(host=args.dbhost, table=args.table,
    #     read_status=args.status, limit=args.messagelimit)
    
    # if len(allmsgs) > 0:
    #     msglist = []
    #     for inbox_id, ts, sim_num, msg in allmsgs:
    #         sms_item = smsclass.SmsInbox(inbox_id, msg, sim_num, str(ts))
    #         msglist.append(sms_item)
         
    #     allmsgs = msglist

    #     try:
    #         parse_all_messages(args,allmsgs)
    #     except KeyboardInterrupt:
    #         print '>> User exit'
    #         sys.exit()

    # else:
    #     print dt.today().strftime("\nServer active as of %A, %B %d, %Y, %X")
    #     return

if __name__ == "__main__":
    main()