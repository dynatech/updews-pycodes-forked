import unittest
import os
import pandas as pd
import ConfigParser
import analysis.querydb as querydb
import analysis.subsurface.filterdata as flt
import analysis.soms.SomsRangeFilter as srf
import json
import ast

def set_cnf(file=''):
    cnffiletxt = file
    cfile = os.path.dirname(os.path.realpath(__file__)) + '/' + cnffiletxt
    cnf = ConfigParser.ConfigParser()
    cnf.read(cfile)

    config_dict = dict()
    for section in cnf.sections():
        options = dict()
        for opt in cnf.options(section):
            options[opt] = cnf.get(section, opt).split(">")
        config_dict[section]= options
    return config_dict


def load_pickle(info):
    data_output = pd.read_pickle(info)
    return data_output


def function_info(function_name,test_id):
    test_case_info = set_cnf(file='unittester_config_ref.cnf')
    info = test_case_info[function_name][test_id]
    info[0] = json.loads(info[0])
    return info


def assert_option(self,info,test_output):
    if str(info[1]).find("//") == "Empty Dataframe":
        data_output = 0
    else:
        data_output = load_pickle(str(info[1]))
        
    if info[2] == "df":
        pd.testing.assert_frame_equal(test_output, data_output,by_blocks=True, 
                                       check_exact=True)
#        print test_output, data_output
    elif info[2] == "str": 
        self.assertEqual(test_output, data_output)

    elif info[2] =="dict":
        self.assertDictEqual(test_output, data_output)


class TestModule(unittest.TestCase):
    """
    - Function get_raw_accel_data test cases
    """  
    
    def test_case_get_raw_accel_data_1(self):
         info = function_info('get_raw_accel_2','test_1')
         df = querydb.get_raw_accel_data_2(tsm_name= str(info[0]['tsm_name']), 
                                         from_time= str(info[0]['from_time']), 
                                         to_time= str(info[0]['to_time']))
         assert_option(self,info,df)


    def test_case_get_raw_accel_data_2(self):
         info = function_info('get_raw_accel_2','test_2')
         df = querydb.get_raw_accel_data_2(tsm_id= int(info[0]['tsm_id']))
         assert_option(self,info,df)


    def test_case_get_raw_accel_data_3(self):
         info = function_info('get_raw_accel_2','test_3')
         df = querydb.get_raw_accel_data_2(tsm_id= int(info[0]['tsm_id']),  
                                         to_time= str(info[0]['to_time']))
         assert_option(self,info,df)
              
                         
    def test_case_get_raw_accel_data_4(self):
         info = function_info('get_raw_accel_2','test_4')
         df = querydb.get_raw_accel_data_2(tsm_id= int(info[0]['tsm_id']), 
                                         from_time= str(info[0]['from_time']),
                                         output_type= str(info[0]['output_type']))
         assert_option(self,info,df)
             
                   
    def test_case_get_raw_accel_data_5(self):
         info = function_info('get_raw_accel_2','test_5')
         df = querydb.get_raw_accel_data_2(tsm_id= int(info[0]['tsm_id']),
                                         to_time= str(info[0]['to_time']),
                                         from_time= str(info[0]['from_time']),
                                         output_type= str(info[0]['output_type']))
         assert_option(self,info,df)


#    ## Comment Test Cases/ Negative
    def test_case_get_raw_accel_data_6(self):
         info = function_info('get_raw_accel_2','test_6')
         with self.assertRaises(ValueError) as context:
            querydb.get_raw_accel_data_2(tsm_id= int(info[0]['tsm_id']))
        
         self.assertTrue('Input tsm_id error' in context.exception)
         
    def test_case_get_raw_accel_data_7(self):
         info = function_info('get_raw_accel_2','test_7')
         with self.assertRaises(ValueError) as context:
            querydb.get_raw_accel_data_2(tsm_name= str(info[0]['tsm_name']))
            
         self.assertTrue('Input tsm_name error' in context.exception)
         
    def test_case_get_raw_accel_data_8(self):
         info = function_info('get_raw_accel_2','test_8')
         with self.assertRaises(ValueError) as context:
            querydb.get_raw_accel_data_2(tsm_id= int(info[0]['tsm_id']),
                                         to_time= str(info[0]['to_time']),
                                         from_time= str(info[0]['from_time']))
            
         self.assertTrue('Input from_time error' in context.exception)


    def test_case_get_raw_accel_data_9(self):
         info = function_info('get_raw_accel_2','test_9')
         with self.assertRaises(ValueError) as context:
            querydb.get_raw_accel_data_2(tsm_name= str(info[0]['tsm_name']), 
                                       from_time= str(info[0]['from_time']), 
                                       accel_number= int(info[0]['accel_number']), 
                                       to_time= str(info[0]['to_time']))
         self.assertTrue('Error accel_number' in context.exception)


    def test_case_get_raw_accel_data_10(self):
         info = function_info('get_raw_accel_2','test_10')

         with self.assertRaises(ValueError) as context:
            querydb.get_raw_accel_data_2(tsm_name= str(info[0]['tsm_name']), 
                                       node_id= int(info[0]['node_id']))
            
         self.assertTrue('Error node_id' in context.exception)
         
         
#    """
#    - Function check_timestamp test cases
#    """
    def test_case_check_timestamp_1(self):
        info = function_info('check_timestamp','test_1')
        df = querydb.check_timestamp(from_time= str(info[0]['from_time']), 
                                     to_time= str(info[0]['to_time']))
        assert_option(self,info,df)


    def test_case_check_timestamp_2(self):
        info = function_info('check_timestamp','test_2')
        df = querydb.check_timestamp(from_time= str(info[0]['from_time']), 
                                     to_time= str(info[0]['to_time']))
        assert_option(self,info,df)
         
         
    def test_case_check_timestamp_3(self):
        info = function_info('check_timestamp','test_3')
        df = querydb.check_timestamp(from_time= str(info[0]['from_time']), 
                                     to_time= str(info[0]['to_time']))
        assert_option(self,info,df)         


    def test_case_check_timestamp_4(self):
        info = function_info('check_timestamp','test_4')
        df = querydb.check_timestamp(from_time= str(info[0]['from_time']), 
                                     to_time= str(info[0]['to_time']))
        assert_option(self,info,df) 


    def test_case_check_timestamp_5(self):
        info = function_info('check_timestamp','test_5')
        df = querydb.check_timestamp(from_time= str(info[0]['from_time']), 
                                     to_time= str(info[0]['to_time']))
        assert_option(self,info,df)        
        
        
    def test_case_check_timestamp_6(self):
         info = function_info('check_timestamp','test_6')
         with self.assertRaises(ValueError) as context:
             querydb.check_timestamp(from_time= str(info[0]['from_time']), 
                                     to_time= str(info[0]['to_time']))
            
         self.assertTrue('Input from_time error' in context.exception)        


    def test_case_check_timestamp_7(self):
         info = function_info('check_timestamp','test_7')
         with self.assertRaises(ValueError) as context:
             querydb.check_timestamp(from_time= str(info[0]['from_time']), 
                                     to_time= str(info[0]['to_time']))
            
         self.assertTrue('Input from_time and to_time error'
         in context.exception) 


    def test_case_check_timestamp_8(self):
         info = function_info('check_timestamp','test_8')
         with self.assertRaises(ValueError) as context:
             querydb.check_timestamp(from_time= str(info[0]['from_time']), 
                                     to_time= str(info[0]['to_time']))
            
         self.assertTrue('Input to_time error' in context.exception)        
         
         
    def test_case_check_timestamp_9(self):
         info = function_info('check_timestamp','test_9')
         with self.assertRaises(ValueError) as context:
             querydb.check_timestamp(from_time= str(info[0]['from_time']), 
                                     to_time= str(info[0]['to_time']))
            
         self.assertTrue('Input from_time error' in context.exception) 

          
    def test_case_check_timestamp_10(self):
         info = function_info('check_timestamp','test_10')
         with self.assertRaises(ValueError) as context:
             querydb.check_timestamp(from_time= str(info[0]['from_time']), 
                                     to_time= str(info[0]['to_time']))
            
         self.assertTrue('Input to_time error' in context.exception)  

    def test_case_check_timestamp_11(self):
        info = function_info('check_timestamp','test_11')
        with self.assertRaises(ValueError) as context:
             querydb.check_timestamp(from_time= str(info[0]['from_time']), 
                                     to_time= str(info[0]['to_time']))
            
        self.assertTrue('Input from_time and to_time error' in context.exception)          

#    """
#    - Function get_tsm_id test cases
#    """  


    def test_case_get_tsm_id_1(self):
        info = function_info('get_tsm_id','test_1')
        df = querydb.get_tsm_id(
                tsm_details = load_pickle(str(info[0]['tsm_details'])), 
                tsm_name = str(info[0]['tsm_name']),
                to_time= str(info[0]['to_time']))
                
        assert_option(self,info,df)    

    def test_case_get_tsm_id_2(self):
        info = function_info('get_tsm_id','test_2')
        df = querydb.get_tsm_id(
                tsm_details = load_pickle(str(info[0]['tsm_details'])), 
                tsm_name = str(info[0]['tsm_name']),
                to_time= str(info[0]['to_time']))
                
        assert_option(self,info,df)   

    def test_case_get_tsm_id_3(self):
        info = function_info('get_tsm_id','test_3')
        df = querydb.get_tsm_id(
                tsm_details = load_pickle(str(info[0]['tsm_details'])), 
                tsm_name = str(info[0]['tsm_name']),
                to_time= str(info[0]['to_time']))
                
        assert_option(self,info,df)   

    def test_case_get_tsm_id_4(self):
        info = function_info('get_tsm_id','test_4')
        df = querydb.get_tsm_id(
                tsm_details = load_pickle(str(info[0]['tsm_details'])), 
                tsm_name = str(info[0]['tsm_name']),
                to_time= str(info[0]['to_time']))
                
        assert_option(self,info,df)   

    def test_case_get_tsm_id_5(self):
        info = function_info('get_tsm_id','test_5')
        df = querydb.get_tsm_id(
                tsm_details = load_pickle(str(info[0]['tsm_details'])), 
                tsm_name = str(info[0]['tsm_name']),
                to_time= str(info[0]['to_time']))
                
        assert_option(self,info,df) 



    def test_case_get_tsm_id_6(self):
        info = function_info('get_tsm_id','test_6')
        with self.assertRaises(ValueError) as context:
             querydb.get_tsm_id(
                tsm_details = load_pickle(str(info[0]['tsm_details'])), 
                tsm_name = str(info[0]['tsm_name']),
                to_time= str(info[0]['to_time']))
      
        self.assertTrue('Input tsm_name error' in context.exception)  
        
        

    def test_case_get_tsm_id_7(self):
        info = function_info('get_tsm_id','test_7')
        with self.assertRaises(AttributeError) as context:
             querydb.get_tsm_id(
                tsm_details = load_pickle(str(info[0]['tsm_details'])), 
                tsm_name = str(info[0]['tsm_name']),
                to_time= str(info[0]['to_time']))
      
        self.assertTrue('\'DataFrame\' object has no attribute \'tsm_id\'' 
        in context.exception)  
        


    def test_case_get_tsm_id_8(self):
        info = function_info('get_tsm_id','test_8')
        with self.assertRaises(ValueError) as context:
             querydb.get_tsm_id(
                tsm_details = load_pickle(str(info[0]['tsm_details'])), 
                tsm_name = str(info[0]['tsm_name']),
                to_time= str(info[0]['to_time']))
      
        self.assertTrue('could not convert string to Timestamp' in context.exception) 
        
        

    def test_case_get_tsm_id_9(self):
        info = function_info('get_tsm_id','test_9')
        with self.assertRaises(ValueError) as context:
             querydb.get_tsm_id(
                tsm_details = load_pickle(str(info[0]['tsm_details'])), 
                tsm_name = str(info[0]['tsm_name']),
                to_time= str(info[0]['to_time']))
      
        self.assertTrue('Input tsm_name error' in context.exception)
        
        

    def test_case_get_tsm_id_10(self):
        info = function_info('get_tsm_id','test_10')
        with self.assertRaises(AttributeError) as context:
             querydb.get_tsm_id(
                tsm_name = str(info[0]['tsm_name']),
                to_time= str(info[0]['to_time']))
        self.assertTrue('\'str\' object has no attribute \'tsm_id\'' 
        in context.exception)
        
        
#    """
#    - Function filter_raw_accel test cases
#    """
    def test_case_filter_raw_accel_1(self):
        info = function_info('filter_raw_accel','test_1')
        df = querydb.filter_raw_accel(
            accel_info = info[0]['accel_info'], 
            query = load_pickle(str(info[0]['query'])),
            df= load_pickle(str(info[0]['df'])))
            
        assert_option(self,info,df) 

    def test_case_filter_raw_accel_2(self):
        info = function_info('filter_raw_accel','test_2')
        df = querydb.filter_raw_accel(
            accel_info = info[0]['accel_info'], 
            query = load_pickle(str(info[0]['query'])),
            df= load_pickle(str(info[0]['df'])))
            
        assert_option(self,info,df) 
        
    def test_case_filter_raw_accel_3(self):
        info = function_info('filter_raw_accel','test_3')
        df = querydb.filter_raw_accel(
            accel_info = info[0]['accel_info'], 
            query = load_pickle(str(info[0]['query'])),
            df= load_pickle(str(info[0]['df'])))
            
        assert_option(self,info,df) 
        
    def test_case_filter_raw_accel_4(self):
        info = function_info('filter_raw_accel','test_4')
        df = querydb.filter_raw_accel(
            accel_info = info[0]['accel_info'], 
            query = load_pickle(str(info[0]['query'])),
            df= load_pickle(str(info[0]['df'])))
            
        assert_option(self,info,df) 
        
    def test_case_filter_raw_accel_5(self):
        info = function_info('filter_raw_accel','test_5')
        df = querydb.filter_raw_accel(
            accel_info = info[0]['accel_info'], 
            query = load_pickle(str(info[0]['query'])),
            df= load_pickle(str(info[0]['df'])))
            
        assert_option(self,info,df) 
         
    """
    - Function get_soms_raw test cases
    """    
    
    
    def test_case_get_soms_raw_positive_1(self):
        info = function_info('ref_get_soms_raw','test_1')
        df = querydb.ref_get_soms_raw(tsm_name=str(info[0]['tsm_name']), 
                                        from_time= str(info[0]['from_time']), 
                                        to_time= str(info[0]['to_time']),
                                        type_num=int(info[0]['type_num']),
                                        node_id=int(info[0]['node_id']))
        assert_option(self,info,df)


    def test_case_get_soms_raw_positive_2(self):
        info = function_info('ref_get_soms_raw','test_2')
        df = querydb.ref_get_soms_raw(tsm_name=str(info[0]['tsm_name']), 
                                        from_time=str(info[0]['from_time']), 
                                        to_time=str(info[0]['to_time']),
                                        type_num=int(info[0]['type_num']),
                                        node_id=int(info[0]['node_id']))
        assert_option(self,info,df)

    
    def test_case_get_soms_raw_positive_3(self):
        info = function_info('ref_get_soms_raw','test_3')
        df = querydb.ref_get_soms_raw(tsm_name=str(info[0]['tsm_name']), 
                                        from_time=str(info[0]['from_time']), 
                                        to_time=str(info[0]['to_time']),
                                        type_num=int(info[0]['type_num']),
                                        node_id=int(info[0]['node_id']))
        data_output = load_pickle(str(info[1]))
        pd.testing.assert_frame_equal(df, data_output,by_blocks=True, 
                          check_exact=True)
                          
                          
    def test_case_get_soms_raw_positive_4(self):
        info = function_info('ref_get_soms_raw','test_4')
        df = querydb.ref_get_soms_raw(tsm_name= str(info[0]['tsm_name']), 
                                        from_time= str(info[0]['from_time']), 
                                        to_time= str(info[0]['to_time']),
                                        type_num= int(info[0]['type_num']),
                                        node_id=  int(info[0]['node_id']))
        assert_option(self,info,df)


    def test_case_get_soms_raw_positive_5(self):
        info = function_info('ref_get_soms_raw','test_5')
        df = querydb.ref_get_soms_raw(tsm_name= str(info[0]['tsm_name']), 
                                        from_time= str(info[0]['from_time']), 
                                        type_num=int(info[0]['type_num']),
                                        node_id=int(info[0]['node_id']))
        assert_option(self,info,df)

    ## Comment Test Cases/ Negative
    def test_case_get_soms_raw_data_6(self):
        info = function_info('ref_get_soms_raw','test_6')

        with self.assertRaises(ValueError) as context:
            querydb.ref_get_soms_raw(tsm_name= str(info[0]['tsm_name']), 
                                            from_time= str(info[0]['from_time']), 
                                            to_time= str(info[0]['to_time']),
                                            type_num= int(info[0]['type_num']),
                                            node_id=  int(info[0]['node_id']))
        
        self.assertTrue('Invalid msgid for version 2 soms sensor. Valid values are 111,112,21,26' in context.exception)


    def test_case_get_soms_raw_data_7(self):
        info = function_info('ref_get_soms_raw','test_7')

        with self.assertRaises(ValueError) as context:
            querydb.ref_get_soms_raw(tsm_name= str(info[0]['tsm_name']),
                                        from_time= str(info[0]['from_time']), 
                                        to_time= str(info[0]['to_time']),
                                        type_num= int(info[0]['type_num']),
                                        node_id=  int(info[0]['node_id']))
        
        self.assertTrue('enter valid tsm_name' in context.exception)


    def test_case_get_soms_raw_8(self):
        info = function_info('ref_get_soms_raw','test_8')
        with self.assertRaises(ValueError) as context:
            querydb.ref_get_soms_raw(tsm_name= str(info[0]['tsm_name']),
                                        from_time= str(info[0]['from_time']), 
                                        to_time= str(info[0]['to_time']),
                                        type_num= int(info[0]['type_num']),
                                        node_id=  int(info[0]['node_id']))
        
        self.assertTrue('Invalid node id. Exceeded number of nodes' in context.exception)
         

    def test_case_get_soms_raw_9(self):
        info = function_info('ref_get_soms_raw','test_9')
        with self.assertRaises(ValueError) as context:
            querydb.ref_get_soms_raw(tsm_name= str(info[0]['tsm_name']),
                                        from_time= str(info[0]['from_time']), 
                                        to_time= str(info[0]['to_time']),
                                        type_num= int(info[0]['type_num']),
                                        node_id=  int(info[0]['node_id']))
        
        self.assertTrue('Invalid msgid for version 3 soms sensor. Valid values are 110,113,10,13' in context.exception)
#
#    """
#    - Function f_outlier test cases
#    """
#
#    def test_case_f_outlier_positive_1(self):
#        info = function_info('f_outlier','test_1')
#        df = srf.f_outlier(df=load_pickle(str(info[0]['df'])), 
#                                        column= str(info[0]['column']), 
#                                        mode=int(info[0]['mode']))                                        
#        assert_option(self,info,df)
#        
#    def test_case_f_outlier_positive_2(self):
#        info = function_info('f_outlier','test_2')
#        df = srf.f_outlier(df=load_pickle(str(info[0]['df'])), 
#                                        column= str(info[0]['column']), 
#                                        mode=int(info[0]['mode']))                                        
#        assert_option(self,info,df)
#
#    def test_case_f_outlier_positive_3(self):
#         info = function_info('f_outlier','test_3')
#
#         with self.assertRaises(IndexError) as context:
#             srf.f_outlier(df=load_pickle(str(info[0]['df'])), 
#                                        column= str(info[0]['column']), 
#                                        mode=int(info[0]['mode']))   
#                                        
#         self.assertTrue('list index out of range' in context.exception)
#
#
#    def test_case_f_outlier_positive_4(self):
#         info = function_info('f_outlier','test_4')
#
#         with self.assertRaises(TypeError) as context:
#             srf.f_outlier(df=load_pickle(str(info[0]['df'])), 
#                                        mode=int(info[0]['mode']))   
#                                        
#         self.assertTrue('f_outlier() takes exactly 3 arguments (2 given)' in context.exception)
#    
#    
#    """
#    - Function f_undervoltage test cases
#    """
#       
#       
#    def test_case_f_undervoltage_positive_1(self):
#        info = function_info('f_undervoltage','test_1')
#        df = srf.f_undervoltage(df=load_pickle(str(info[0]['df'])), 
#                                        column= str(info[0]['column']), 
#                                        node=int(info[0]['node']))                                        
#        assert_option(self,info,df)
#        
#        
#    def test_case_f_undervoltage_positive_2(self):
#        info = function_info('f_undervoltage','test_2')
#        df = srf.f_undervoltage(df=load_pickle(str(info[0]['df'])), 
#                                        column= str(info[0]['column']), 
#                                        node=int(info[0]['node']))                                        
#        assert_option(self,info,df)
#
#    
#    
#    """
#    - Function apply_filters test cases
#    """
#    
#    
#    def test_case_apply_filters_1(self):
#         info = function_info('apply_filters','test_1')
#         df = flt.apply_filters(dfl=load_pickle(str(info[0]['dfl'])))
#         assert_option(self,info,df)    
#
#
#
#    def test_case_apply_filters_2(self):
#         info = function_info('apply_filters','test_2')
#         df = flt.apply_filters(dfl=load_pickle(str(info[0]['dfl'])))
#         assert_option(self,info,df)     
#
#
#    def test_case_apply_filters_3(self):
#         info = function_info('apply_filters','test_3')
#         df = flt.apply_filters(dfl=load_pickle(str(info[0]['dfl'])))
#         assert_option(self,info,df)         
#
#
#    def test_case_apply_filters_4(self):
#         info = function_info('apply_filters','test_4')
#         df = flt.apply_filters(dfl=load_pickle(str(info[0]['dfl'])))
#         assert_option(self,info,df) 
#
#
#    def test_case_apply_filters_5(self):
#         info = function_info('apply_filters','test_5')
#         df = flt.apply_filters(dfl=load_pickle(str(info[0]['dfl'])))
#         assert_option(self,info,df)    
#         
#         
#    """
#    - Function orthogonal_filter test cases
#    """
#    
#    def test_case_orthogonal_filter_1(self):
#         info = function_info('orthogonal_filter','test_1')
#         df = flt.orthogonal_filter(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df)      
#
#
#    def test_case_orthogonal_filter_2(self):
#         info = function_info('orthogonal_filter','test_2')
#         df = flt.orthogonal_filter(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df)     
#
#
#    def test_case_orthogonal_filter_3(self):
#         info = function_info('orthogonal_filter','test_3')
#         df = flt.orthogonal_filter(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df)         
#
#
#    def test_case_orthogonal_filter_4(self):
#         info = function_info('orthogonal_filter','test_4')
#         df = flt.orthogonal_filter(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df) 
#
#
#    def test_case_orthogonal_filter_5(self):
#         info = function_info('orthogonal_filter','test_5')
#         df = flt.orthogonal_filter(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df)      
#    
#    
#    """
#    - Function outlier_filter test cases
#    """
#    
#    def test_case_outlier_filter_1(self):
#         info = function_info('outlier_filter','test_1')
#         df = flt.outlier_filter(dff=load_pickle(str(info[0]['dff'])))
#         assert_option(self,info,df)      
#
#
#    def test_case_outlier_filter_2(self):
#         info = function_info('outlier_filter','test_2')
#         df = flt.outlier_filter(dff=load_pickle(str(info[0]['dff'])))
#         assert_option(self,info,df)     
#
#
#    def test_case_outlier_filter_3(self):
#         info = function_info('outlier_filter','test_3')
#         df = flt.outlier_filter(dff=load_pickle(str(info[0]['dff'])))
##         print load_pickle(str(info[0]['dff']))
#         assert_option(self,info,df)         
#
#
#    def test_case_outlier_filter_4(self):
#         info = function_info('outlier_filter','test_4')
#         df = flt.outlier_filter(dff=load_pickle(str(info[0]['dff'])))
#         assert_option(self,info,df) 
#
#
#    def test_case_outlier_filter_5(self):
#         info = function_info('outlier_filter','test_5')
#         df = flt.outlier_filter(dff=load_pickle(str(info[0]['dff'])))
#         assert_option(self,info,df)      
#    
#    
#    """
#    - Function range_filter_accel test cases
#    """
#    
#    def test_case_range_filter_accel_1(self):
#         info = function_info('range_filter_accel','test_1')
#         df = flt.range_filter_accel(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df)      
#
#
#    def test_case_range_filter_accel_2(self):
#         info = function_info('range_filter_accel','test_2')
#         df = flt.range_filter_accel(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df)     
#
#
#    def test_case_range_filter_accel_3(self):
#         info = function_info('range_filter_accel','test_3')
#         df = flt.range_filter_accel(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df)         
#
#
#    def test_case_range_filter_accel_4(self):
#         info = function_info('range_filter_accel','test_4')
#         df = flt.range_filter_accel(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df) 
#
#
#    def test_case_range_filter_accel_5(self):
#         info = function_info('range_filter_accel','test_5')
#         df = flt.range_filter_accel(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df)      
#    
#           
#    """
#    - Function resample_df test cases
#    """
# 
#    
#    def test_case_resample_df_1(self):
#         info = function_info('resample_df','test_1')
#         df = flt.resample_df(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df)      
#
#
#    def test_case_resample_df_2(self):
#         info = function_info('resample_df','test_2')
#         df = flt.resample_df(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df)     
#
#
#    def test_case_resample_df_3(self):
#         info = function_info('resample_df','test_3')
#         df = flt.resample_df(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df)         
#
#
#    def test_case_resample_df_4(self):
#         info = function_info('resample_df','test_4')
#         df = flt.resample_df(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df) 
#
#
#    def test_case_resample_df_5(self):
#         info = function_info('resample_df','test_5')
#         df = flt.resample_df(df=load_pickle(str(info[0]['df'])))
#         assert_option(self,info,df)      
#    
#    
#    """
#    - Function ref_get_soms_raw test cases
#    """
       
                     
def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestModule)
    unittest.TextTestRunner(verbosity=2).run(suite)

    
if __name__ == "__main__":
    main()