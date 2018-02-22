import serverdbio as dbio
from datetime import datetime as dt
from datetime import timedelta as td
import argparse

def count_items(ts_end = None, ts_start = None, table = None, stat_col = None,
	stat = None, pq = False, write_to_db = False, timelag = 5, 
	backtrack = 5000, dbinstance = 'local'):

	if table is None:
		raise ValueError("No table given")
	else:
		if table.find("outbox") > 0:
			index = "outbox_id"
		elif table.find("inbox") > 0:
			index = "inbox_id"
		else:
			raise ValueError("Unknown table %s" % table)

	if ts_end is None:
		ts = dt.now()
		ts_end = ts - td(minutes = ts.minute % timelag, seconds = ts.second, 
			microseconds = ts.microsecond)
		
		if ts_start is None:
			ts_start = ts_end - td(minutes = timelag)
	
		ts_start = ts_start.strftime("%Y-%m-%d %H:%M:%S")
		ts_end = ts_end.strftime("%Y-%m-%d %H:%M:%S")

	print "Count %s items from %s to %s:" % (table, ts_start, ts_end)

	query = ("select count(%s) from %s "
		"where %s > (select max(%s) - %d from %s) "
		"and ts_stored >= '%s' "
		"and ts_stored < '%s' " ) % (index, table, index, index, backtrack, 
		table, ts_start, ts_end)

	if stat_col is not None:
		if stat is None:
			raise ValueError('No stat for stat_col (%s) given' % (stat_col))
		query += "and %s like '%s%s'" % (stat_col,stat,'%')
		
	else:
		stat_col = 'undefined'
		stat = "undefined"

	if pq:
		print "Count query:", query

	rs = dbio.query_database(query, "ci", "sandbox")

	try:
		item_count = rs[0][0]
		print "Count: %d" % (item_count)
	except ValueError:
		print "Error in resultset conversion. %s", rs
		return None
	except IndexError, TypeError:
		print "Error: Query error (%s)" % (query)
		return None

	# return item_count

	if write_to_db:
		table_dict = {"smsinbox_loggers": 1, "smsoutbox_loggers": 2,
			"smsinbox_users": 3, "smsoutbox_users": 4}
		stat_dict = {"undefined": 0, "read": 1, "unread": 2, "sent": 3, 
			"unsent": 4, "fatal error": 5, "read-fail": 6, "read-success": 7}
		col_dict = {"undefined": 0, "read_status": 1, "send_status": 2}

		query = ("insert into item_count_logs (`table`, `ts_end`, `ts_start`, "
			"`stat`, `stat_col`, `count`) values (%d, '%s', '%s', %d, %d, "
			"%d) on duplicate key update `count` = values(`count`)" % ( 
				table_dict[table], ts_end, ts_start, stat_dict[stat], 
				col_dict[stat_col], item_count))

		if pq:
			print "Write query:", query

		dbio.commit_to_db(query, 'item_count', dbinstance)

def get_arguments():
    parser = argparse.ArgumentParser(description=("Run performance "
    	"monitoring routines [-options]"))
    parser.add_argument("-t", "--table", 
        help="table to count items from")
    parser.add_argument("-c", "--stat_col", 
        help="column of status")
    parser.add_argument("-s", "--stat", 
        help="status of item")
    parser.add_argument("-r", "--ts_start", 
        help="start time of count")
    parser.add_argument("-e", "--ts_end", 
        help="end time of count")
    parser.add_argument("-i", "--instance", 
        help="what machine to write result into")
    parser.add_argument("-w", "--write_to_db", 
        help="write result to db", action="store_true")
    parser.add_argument("-q", "--print_query", 
        help="print query", action="store_true")
    parser.add_argument("-l", "--time_lag", 
        help="time (mins) from ts_end to count items",
        type=int)

    try:
        args = parser.parse_args()

        if args.table == None:
            args.status = 0
        return args        

    except IndexError:
        print '>> Error in parsing arguments'
        error = parser.format_help()
        print error
        sys.exit()


def main():

	args = get_arguments()

	count_items(args.ts_end, args.ts_start, args.table, args.stat_col, 
		args.stat, args.print_query, args.write_to_db, args.time_lag)

if __name__ == "__main__":
    main()