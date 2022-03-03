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

optest = operator_test(__file__)

# config parameter
api.config.errorHandling = '{"type":"terminate on error"}' # type: string


api.config.couchdb = { 'connectionProperties':
                           {'host' : 'https://mcs.gauselmann.com:5984',
                            'user' : 'sap_poc',
                            'password': 'sap__checkIn_2022',
                            'path':'statistics'
                            }
                       }

api.config.startkey = '2022/02/19' # type: string
api.config.endkey = '2022/02/20' # type: string
api.config.view = '_design/sap_analytics/_view/visits' # type: string


script.gen()

#for m in api.msg_list :
#    print(f"**** Port: {m['port']} ******")
#    print(m['msg'])


result = json.loads(api.msg_list[0]['msg'])

rows = result['total_rows']
offset = result['offset']
records = [ v['value'] for v in result['rows']]

df = pd.DataFrame(records)
col_map = { c : c[6:].upper() for c in df.columns}
df.rename(columns=col_map,inplace=True)

for c in df.columns :
    print(f'{c} -{df[c].dtype}')

#print(df)

