import script
from utils.mock_di_api import mock_api
from utils.operator_test import operator_test
        
api = mock_api(__file__)     # class instance of mock_api
mock_api.print_send_msg = True  # set class variable for printing api.send

optest = operator_test(__file__)
# config parameter 
api.config.period = 1   # datatype : integer
api.config.pipeline = 'p1'   # datatype: integer

i = 0
while i < 100 :
  script.gen()
  i +=1


  