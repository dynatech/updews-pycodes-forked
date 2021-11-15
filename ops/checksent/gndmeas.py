from datetime import datetime, timedelta
import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import volatile.memory as mem


def main(time_now=datetime.now()):
    conn = mem.get('DICT_DB_CONNECTIONS')
    query  = "select site_code, ts, marker_name from  "
    query += "  (select data_id from {analysis}.marker_data_tags "
    query += "  where tag_type = 0 "
    query += "  ) tag "
    query += "inner join (select data_id, alert_level from {analysis}.marker_alerts) sub1 using (data_id) "
    query += "inner join {analysis}.marker_data using (data_id) "
    query += "inner join {analysis}.marker_observations mo using (mo_id) "
    query += "inner join {common}.sites using (site_id) "
    query += "inner join (select marker_id, marker_name from {analysis}.view_marker_history) sub2 using (marker_id)"
    query += "where alert_level = 0 "
    query += "and mo.ts >= '{ts}' "
    query = query.format(analysis=conn['analysis']['schema'], common=conn['common']['schema'], ts=time_now-timedelta(1.5))
    tags = db.df_read(query, resource='sensor_analysis')
    tags.loc[:, 'ts'] = tags.loc[:, 'ts'].astype(str)

    if len(tags) != 0:
        msg = 'Validate measurements with displacement of 1cm and more:\n'
        msg += '\n'.join(list(map(lambda x: ': '.join(x), tags.values)))
    else:
        msg = ''

    return msg