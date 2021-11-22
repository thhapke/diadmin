import sys
import yaml
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


api.config.hierarchy = 'License'
api.config.tenant = "default"
api.config.http_connection = optest.get_json('http_connection.json')
print(api.config.http_connection)
script.gen()
