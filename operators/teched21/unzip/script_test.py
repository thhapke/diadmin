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

zipfile = 'tageswerte_KL_00044_akt.zip'
msg = optest.get_msgfile(zipfile)
msg.attributes['zipfile'] = zipfile

script.on_input(msg)

optest.save_csvfile('y.csv',api.msg_list[0]['data'])