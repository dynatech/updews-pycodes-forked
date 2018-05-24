import sys
import os
import warnings

from datetime import datetime

import dynadb.db as dynadb
import volatile.memory as memory


class VariableInfo:
    def __init__(self, info):   
        self.name = str(info[0])
        self.query = str(info[1])
        self.type = str(info[2])
        self.index_id = str(info[3])
        
        
def dict_format(query_string, variable_info):
    query_output = dynadb.read(query_string)
    if query_output:
        dict_output = {a: b for a, b 
        in query_output}
        return dict_output
    else:
        return False


def set_static_variable(name=""):
    query = "Select name, query, data_type, "
    query += "ts_updated from static_variables"
    date = datetime.now()
    date = date.strftime('%Y-%m-%d %H:%M:%S')
    if name != "":
        query += " where name = '%s'" % (name)
    
    variables = dynadb.read(
      query=query,
      identifier='Set static_variables')
    
    for data in variables:
        variable_info = VariableInfo(data)
        query_string = variable_info.query
        
        if variable_info.type == 'data_frame':
            static_output = dynadb.df_read(query_string)

        elif variable_info.type == 'dict':
            static_output = dict_format(
              query_string, 
              variable_info)
        else:
            static_output = dynadb.read(query_string)
            
        if len(static_output) == 0 : 
            warnings.warn('Query Error' + variable_info.name)
        else:
            memory.set(variable_info.name, static_output)
            query_ts_update = "UPDATE static_variables SET "
            query_ts_update += " ts_updated ='%s' " %(date)
            query_ts_update += " WHERE name ='%s'" % (
                variable_info.name)
            dynadb.write(query_ts_update)
            print variable_info.name
        
def main():
    set_static_variable()

if __name__ == "__main__":
    main()