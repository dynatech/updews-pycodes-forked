import sys
import os
import dynadb.db as dynadb
import volatile.memory as memory

class Variableinfo:
       
  def __init__(self, info):   
    self.name = str(info[1])
    self.query = str(info[2])
    self.type = str(info[3])
    self.format = str(info[4])
    self.column_list =  str(info[5])
    self.index_id = str(info[6])
        

def dict_format(query_string,variable_info):

    query_output = dynadb.read(query_string)
    data_format = variable_info.format[:-1][1:].replace(":",",").replace("\'","")

    dict_output = {a: b for a , b in query_output}

    return dict_output

def query_static():

    query = 'Select * from static_variables'
    variables = dynadb.read(query = query, 
        identifier = 'Get all static_variables')


    for data in variables:

        variable_info = Variableinfo(data)
        query_string = variable_info.query.replace("LECT ",
            "LECT "+ variable_info.column_list +" ")

        if variable_info.type == 'data_frame':
           df_query_output = dynadb.df_read(query_string)
           memory.set(variable_info.name, df_query_output)
           print variable_info.name

        elif variable_info.type == 'dict':
           data_dict = dict_format(query_string,variable_info)
           memory.set(variable_info.name, data_dict)
           print variable_info.name

        else:
           df_query_output = dynadb.read(query_string)
           memory.set(df_query_output, data_dict)
           print variable_info.name

def main():
    query_static()

if __name__ == "__main__":
    main()