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

api.init(__file__)  # class instance of mock_api

# config parameter
with open(join('http_connections', 'http_connection.json')) as fp:
    api.config.http_connection = json.load(fp)

# config parameter
# api.config.connection_id = 'HANA_Cloud_DQM'
# api.config.connection_id = {'connectionID': 'S3_Catalog'}
# api.config.connection_id = 'ECC_DMIS_2018'
# api.config.connection_id = 'HANA_Cloud_DQM'
# api.config.container = '/TABLES/BC'
# api.config.container = '/QMGMT'
# api.config.container = '/costcenter'
#api.config.connection_id = {'connectionID': 'DI_DATA_LAKE'}
#api.config.container = '/shared/dataqm/SAPCC/instances'
api.config.connection_id = {'connectionID': 'S4HANA'}
api.config.container = '/TABLES'
api.config.streaming = False
api.config.tags = True
api.config.lineage = True

script.gen()

result_str = ""
# result_str += "["
for m in api.msg_list:
    print(m['msg'])
    result_str += m['msg']+',\n'

# result_str = result_str[:-2]+']\n'
result_str = result_str[:-2]+'\n'
with open(f"tmp/datasets_{api.config.connection_id['connectionID']}.json", 'w') as fp:
    fp.write(result_str)
