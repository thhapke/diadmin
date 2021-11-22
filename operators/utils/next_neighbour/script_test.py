import sys
from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import script
from diadmin.dimockapi.mock_api import mock_api
from diadmin.dimockapi.mock_inport import operator_test

api = mock_api(__file__)     # class instance of mock_api
mock_api.print_send_msg = True  # set class variable for printing api.send

optest = operator_test(__file__)

# config parameter
api.config.dim_data = ['LONGITUDE', 'LATITUDE'] # type: array
api.config.id_net = 'STATIONS_ID'
api.config.dim_net = ['LONGITUDE', 'LATITUDE'] # type: array
api.config.suffix_data = '_DEVICE'
api.config.suffix_net = '_STATION'


file = 'dwdstations.csv'
msg_net = optest.get_msgfile(file)

file = 'devices.csv'
msg_main = optest.get_msgtable(file)

script.on_input(msg_net,msg_main)
