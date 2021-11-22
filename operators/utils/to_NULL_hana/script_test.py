import sys
import logging
from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)
        
import script
from utils.mock_di_api import mock_api
from utils.operator_test import operator_test
        
api = mock_api(__file__)     # class instance of mock_api
mock_api.print_send_msg = True  # set class variable for printing api.send

optest = operator_test(__file__)
# config parameter 
api.config.str_value = 'XX'   # datatype : string
api.config.num_value = '-999'    # datatype : number
api.config.str_columns =['cols1','cols2']
api.config.num_columns =['coln1','coln2']
api.config.table_name ='test_table'

logging.basicConfig(level=logging.INFO,format='%(levelname)s:%(message)s')
msg = optest.get_msgtable('devices.csv')
script.gen()


