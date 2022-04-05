import sys
import json
import re
import pandas as pd
from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import script
from diadmin.dimockapi.mock_api import api
from diadmin.dimockapi.mock_inport import operator_test

api.init(__file__)     # class instance of mock_api

# config parameter
with open('../../../../http_connections/http_connection_usa.json') as fp:
    api.config.http_connection = json.load(fp)
print(api.config.http_connection)

# config parameter
api.config.connection_id = 'HANA_Cloud_DQM'
api.config.connection_id = "ECC_IDES_GCP"
api.config.container = '/'
api.config.container = '/TABLES/SD/SD-SLS'

script.gen()

for m in api.msg_list:
    print(json.dumps(m['msg'],indent=4))