import sys
from os.path import dirname, join, abspath
import logging
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)
        
import script
from utils.mock_di_api import mock_api
from utils.operator_test import operator_test
        
api = mock_api(__file__,False)     # class instance of mock_api

#api.print_send_msg = False  # set class variable for printing api.send
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
optest = operator_test(__file__)


blob = optest.get_blob('KL_Tageswerte_Beschreibung_Stationen.txt')
script.on_input(blob)
api.config.only_running = True

optest.msgfile2file(api.msg_list[0]['data'],'dwdstations.csv')

