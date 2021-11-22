import sys
from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)
        
import script
import pandas as pd

from utils.mock_di_api import mock_api
from utils.operator_test import operator_test
        
api = mock_api(__file__)     # class instance of mock_api
mock_api.print_send_msg = True  # set class variable for printing api.send

optest = operator_test(__file__)
# config parameter 



msg_stations = optest.get_msgfile('dwdstations.csv')

msg_devices = optest.get_msgtable('devices.csv')

# Reduce original table to columns needed
header = [c['name'] for c in msg_devices.attributes['table']['columns']]
df = pd.DataFrame(msg_devices.body, columns=header)
df = df[['SERIAL_NO','LATITUDE','LONGITUDE']]
new_device_file = optest._filename('devices_reduced.csv')
df.to_csv(new_device_file,index=False)

msg_devices = optest.get_msgtable('devices_reduced.csv')
msg = api.Message(attributes={'operator':'teched21.next_wstation'},body = None)

script.on_input(msg_stations,msg_devices)

