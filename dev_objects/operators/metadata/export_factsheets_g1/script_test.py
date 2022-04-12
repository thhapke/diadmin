import sys
import json
from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import script
from diadmin.dimockapi.mock_api import api


# config parameter
with open('http_connections/http_connection.json') as fp:
    api.config.http_connection = json.load(fp)
print(api.config.http_connection)

api.config.connection_id = 'HANA_Cloud_DQM'
api.config.path = '/QMGMT/BARCELONA_CSKB_PREP'
api.config.lineage = True
api.config.verify = False

script.gen()
