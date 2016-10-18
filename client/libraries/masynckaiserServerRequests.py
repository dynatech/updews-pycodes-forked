import common

#Show the schemas available from the masynckaiser server
#Note: even though this information is sent to the client, the schema 
#   permissions from the masynckaiser_schema_targets table will still apply
def showSchemas():
    request = """{"dir":0,"action":"read","query":"SHOW DATABASES"}"""
    return request

#Show the tables from the schema of interest
#Note: even though this information is sent to the client, the table 
#   permissions from the masynckaiser_table_permissions table will still apply
def showTables(schema=None):
    if not schema:
        msgError = "%s ERROR: No schema selected" % (common.whoami())
        print msgError
        return None
    
    request = """{"dir":0,"action":"read","query":"SHOW TABLES",
                  "schema":"%s"}""" % (schema)
    return request
    
def getTableConstructionCommand(schema, table):
    if (not schema) or (not table):
        msgError = "%s ERROR: No schema or table selected" % (common.whoami())
        print msgError
        return None
    
    request = """{"dir":0,"action":"read",
                  "query":"SHOW CREATE TABLE %s",
                  "schema":"%s"}""" % (table, schema)
    return request
    
def showPrimaryKey(schema, table):
    if (not schema) or (not table):
        msgError = "%s ERROR: No schema or table selected" % (common.whoami())
        print msgError
        return None
    
    request = """{"dir":0,"action":"read",
                  "query":"SHOW INDEX FROM %s",
                  "schema":"%s"}""" % (table, schema)
    return request