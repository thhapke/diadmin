import sys
import logging
from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import requests

import script
from utils.mock_di_api import mock_api
from utils.operator_test import operator_test
        
api = mock_api(__file__,print_flag=False)     # class instance of mock_api
mock_api.print_send_msg = True  # set class variable for printing api.send

optest = operator_test(__file__)
# config parameter

msg = optest.get_msgtable('dwdstations.csv')
script.on_input(msg)

for i,m in enumerate(api.msg_list) :
    filename = m['data'].attributes['zipfile']
    logging.info(f"Download file: {filename}")
    r = requests.get(m['data'].attributes['http.url'])
    optest.save_bytefile(filename,r.content)
    #if i == 2 :
    #    break


