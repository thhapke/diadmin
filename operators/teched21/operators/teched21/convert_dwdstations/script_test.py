import sys
from os.path import dirname, join, abspath
import logging
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)
        
import script
from utils.mock_di_api import mock_api
from utils.operator_test import operator_test
        
api = mock_api(__file__)     # class instance of mock_api

mock_api.print_send_msg = True  # set class variable for printing api.send
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
optest = operator_test(__file__)
# config parameter

data_dir = '/Users/Shared/data/Teched2021/'
infile = 'KL_Tageswerte_Beschreibung_Stationen.txt'
outfile = 'stations.csv'

with open(join(data_dir,infile),'rb') as fp :
  blob = fp.read()


#msg = api.Message(attributes={'operator':'teched21.convert_dwdstations'},body = text)
script.on_input(blob)



