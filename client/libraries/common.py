import sys
import json
import simplejson

def whoami():
    return sys._getframe(1).f_code.co_name

#Parse the json message and return as an array
def parseBasicList(payload, withKey=False):
    msg = format(payload.decode('utf8'))
    parsed_json = json.loads(json.loads(msg))
    
    if withKey:
        return parsed_json
    else:
        schemaList = []
        for json_dict in parsed_json:
            for key,value in json_dict.iteritems():
    #            print("key: {} | value: {}".format(key, value))
                schemaList.append(value)
                
        return schemaList

def parseTableCreationCommand(payload):
    msg = format(payload.decode('utf8'))
    parsed_json = json.loads(json.loads(msg))
    
    schemaList = []
    for json_dict in parsed_json:
        for key,value in json_dict.iteritems():
#            print("key: {} | value: {}".format(key, value))
            schemaList.append(value)
            
#    print schemaList[1]
    return schemaList[1]