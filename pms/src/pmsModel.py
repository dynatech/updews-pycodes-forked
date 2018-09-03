import pymysql as mysqlDriver
import pandas.io.sql as psql
import sys
import datetime
import volatile.memory as mem
from pprint import pprint

def connectDatabase(hostdb='local'):
    sc = mem.server_config()
    Hostdb = "localhost"
    Userdb = "root"
    Passdb = "senslope"
    Namedb = "performance_monitoring"
    while True:
        try:
            db = mysqlDriver.connect(host = Hostdb, user = Userdb, 
                passwd = Passdb, db=Namedb)
            cur = db.cursor()
            cur.execute("use "+ Namedb)
            return db, cur
        except mysqlDriver.OperationalError:
            print_out('.')

def getDataFrame(query, hostdb='local'):
    try:
        db, cur = connectDatabase(hostdb)
        df = psql.read_sql(query, db)
        db.close()
        return df
    except KeyboardInterrupt:
        print_out("Exception detected in accessing database")

def executeQuery(query, hostdb='local'):
    try:
        db, cur = connectDatabase(hostdb)
        cur.execute(query)
        db.commit()
        db.close()
        status = True
    except Exception as e:
        status = False
    return status

def getMetric(metric_name):
    query = "SELECT metric_id FROM metrics WHERE metric_name = '%s';" %metric_name
    result = getDataFrame(query)
    return result

def getTableReference(table_name):
    query = "SELECT table_id FROM table_references WHERE table_name = '%s';" %table_name
    result = getDataFrame(query)
    if len(result) == 0:
        insertTableReference(table_name)
        query = "SELECT table_id FROM table_references WHERE table_name = '%s';" %table_name
        result = getDataFrame(query)
    else:
        query = "SELECT table_id FROM table_references WHERE table_name = '%s';" %table_name
        result = getDataFrame(query)
    return result

def insertTableReference(table_name):
    query = "INSERT INTO table_references VALUES (0,'%s');" %table_name
    result = executeQuery(query)
    return result

def insertAccuracy(report):
    try:
        reference_table = getTableReference(report['reference_table'])
        reference_id = report['reference_id']
        metric_id_container = getMetric(report['metric_name'])
        metric_id = metric_id_container['metric_id'][0]
        report_message = report['report_message']
        query = "INSERT INTO accuracy VALUES ('0','%s','%s','%s','%s','%s');"\
         %(metric_id, report['ts'], report_message, reference_id, reference_table['table_id'][0])
        result = executeQuery(query)
        status = {
            "status": result
        }
    except Exception as e:
        status = {
            "status": False,
            "message": str(e) + ": Please check if metric_name, reference_id, reference_table, submetrics, report_message and ts is set"
        }
    return status

def insertAccuracyWithSubmetric(report):
    try:
        report_submit = insertAccuracy(report)
        metric_id_container = getMetric(report['metric_name'])
        metric_id = metric_id_container['metric_id'][0]
        for sub in report['submetrics']:
            check_if_exists_query = "SELECT * FROM submetrics WHERE metric_id = '%s';" %metric_id
            check_if_exists = getDataFrame(check_if_exists_query)

            fields_query = "SHOW columns FROM %s" %check_if_exists['submetric_table_name'][0]
            fields = getDataFrame(fields_query)

            field_names = ""
            counter = 0
            for field in fields['Field']:
                if field != "instance_id" and field != "metric_ref_id":
                    if counter == 0:
                        if field == sub:
                            field_names = field_names + "1"
                            counter = counter + 1
                        else:
                            field_names = field_names + "0"
                    else:
                        if field == sub:
                            field_names = field_names + ",1"
                            counter = counter + 1
                        else:
                            field_names = field_names + ",0"

            insert_sub_metric = "INSERT INTO %s VALUES (0,'%s',%s)" %(check_if_exists['submetric_table_name'][0],metric_id,field_names)
            result = executeQuery(insert_sub_metric)
            status = {
                "status": result
            }

    except Exception as e:
        status = {
            "status": False,
            "message": str(e) + ": Please check if metric_name, reference_id, reference_table, submetrics, report_message and ts is set"
        }
    return status


def insertTimeliness(report):
    try:
        reference_table = getTableReference(report['reference_table'])
        metric_id_container = getMetric(report['metric_name'])
        metric_id = metric_id_container['metric_id'][0]
        report_message = report['report_message']

        query = "INSERT INTO timeliness VALUES ('0','%s','%s','%s');"\
         %(metric_id, report['ts'], execution_time, report_message, report['reference_id'], reference_table['table_id'][0])
        result = executeQuery(query)
        status = {
            "status": result
        }
    except Exception as e:
        status = {
            "status": False,
            "message": str(e) + ": Please check if metric_name, reference_id, reference_table, submetrics, report_message and ts is set"
        }

    return status

def insertTimelinessWithSubmetric(report):
    try:
        report_submit = insertTimeliness(report)
        metric_id_container = getMetric(report['metric_name'])
        metric_id = metric_id_container['metric_id'][0]
        for sub in report['submetrics']:
            check_if_exists_query = "SELECT * FROM submetrics WHERE metric_id = '%s';" %metric_id
            check_if_exists = getDataFrame(check_if_exists_query)

            fields_query = "SHOW columns FROM %s" %check_if_exists['submetric_table_name'][0]
            fields = getDataFrame(fields_query)

            field_names = ""
            counter = 0
            for field in fields['Field']:
                if field != "instance_id" and field != "metric_ref_id":
                    if counter == 0:
                        if field == sub:
                            field_names = field_names + "1"
                            counter = counter + 1
                        else:
                            field_names = field_names + "0"
                    else:
                        if field == sub:
                            field_names = field_names + ",1"
                            counter = counter + 1
                        else:
                            field_names = field_names + ",0"

            insert_sub_metric = "INSERT INTO %s VALUES (0,'%s',%s)" %(check_if_exists['submetric_table_name'][0],metric_id,field_names)
            result = executeQuery(insert_sub_metric)
            
        status = {
            "status": result
        }

    except Exception as e:
        status = {
            "status": False,
            "message": str(e) + ": Please check if metric_name, reference_id, reference_table, submetrics, report_message and ts is set"
        }
    return status

def insertErrorLog(report):
    try:
        reference_table = getTableReference(report['reference_table'])
        metric_id_container = getMetric(report['metric_name'])
        metric_id = metric_id_container['metric_id'][0]
        report_message = report['report_message']

        query = "INSERT INTO error_logs VALUES ('0','%s','%s','%s','%s','%s');"\
         %(metric_id, report['ts'], report_message, report['reference_id'], reference_table['table_id'][0])
        result = executeQuery(query)
        status = {
            "status": result
        }
    except Exception as e:
        status = {
            "status": False,
            "message": str(e) + ": Please check if metric_name, reference_id, reference_table, submetrics and ts is set"
        }

    return status

def insertErrorLogWithSubmetric(report):
    try:
        report_submit = insertErrorLog(report)
        metric_id_container = getMetric(report['metric_name'])
        metric_id = metric_id_container['metric_id'][0]
        for sub in report['submetrics']:
            check_if_exists_query = "SELECT * FROM submetrics WHERE metric_id = '%s';" %metric_id
            check_if_exists = getDataFrame(check_if_exists_query)

            fields_query = "SHOW columns FROM %s" %check_if_exists['submetric_table_name'][0]
            fields = getDataFrame(fields_query)

            field_names = ""
            counter = 0
            for field in fields['Field']:
                if field != "instance_id" and field != "metric_ref_id":
                    if counter == 0:
                        if field == sub:
                            field_names = field_names + "1"
                            counter = counter + 1
                        else:
                            field_names = field_names + "0"
                    else:
                        if field == sub:
                            field_names = field_names + ",1"
                            counter = counter + 1
                        else:
                            field_names = field_names + ",0"

            insert_sub_metric = "INSERT INTO %s VALUES (0,'%s',%s)" %(check_if_exists['submetric_table_name'][0],metric_id,field_names)
            result = executeQuery(insert_sub_metric)
            status = {
                "status": result
            }

    except Exception as e:
        status = {
            "status": False,
            "message": str(e) + ": Please check if metric_name, reference_id, reference_table, submetrics and ts is set"
        }

    return status