import sys
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
api.config.num_devices = 100   # datatype : integer
api.config.num_device_types = 7   # datatype : integer
api.config.prefix_device = 'GRO-'    # datatype : string
api.config.start_date = '2014-03-02'    # datatype : string

file = 'dwdstations.csv'
msg = optest.get_msgfile(file)

script.on_input(msg)
# print result list

