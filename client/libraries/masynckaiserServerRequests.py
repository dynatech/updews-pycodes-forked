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
    
#Compose the message for requesting data for updating a database table
#   PKeysValsJson is a json string of 
#TODO: Make this resilient to Multi Primary Key Tables
def getDataUpdateCommand(schema, table, PKeysValsJson, limit = 1000):
    if (not schema) or (not table):
        msgError = "%s ERROR: No schema or table selected" % (common.whoami())
        print msgError
        return None
        
    #TODO: Error message if PKeysValsJson is None or not a JSON
        
    keys = []
    values = []
    for key,value in PKeysValsJson.iteritems():
        print("key: {} | value: {}".format(key, value))
        keys.append(key)
        values.append(value)
        
    if not values[0]:
        query = """
                SELECT * 
                FROM %s 
                ORDER BY %s asc 
                LIMIT %s
                """ % (table, keys[0], limit)
    else:
        query = """
                SELECT * 
                FROM %s 
                WHERE %s >= '%s' 
                ORDER BY %s asc 
                LIMIT %s
                """ % (table, keys[0], values[0], keys[0], limit)
            
#    print "%s Query: %s" % (common.whoami(), query)

    request = """{"dir":0,"action":"read",
                  "query":"%s",
                  "schema":"%s"}""" % (query, schema)

#    print "%s Request: %s" % (common.whoami(), request)
    return request





















