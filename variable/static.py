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
  data_format = variable_info.format[:-1][1:]
  data_format = data_format.replace(":",",")
  data_format = data_format.replace("\'","")

  dict_output = {a: b for a , b in query_output}

  return dict_output

def set_static_variable(name = ""):

  query = "Select * from static_variables"

  if name != "":
    query += " where name = '%s'" %(name)

  variables = dynadb.read(query = query, 
      identifier = 'Set static_variables')

  for data in variables:
    format_data(data)


def format_data(variable):

    variable_info = Variableinfo(variable)
    query_string = variable_info.query.replace(
      'LECT ','LECT '+ variable_info.column_list +' ')

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
  set_static_variable()

if __name__ == "__main__":
  main()