import ntpath
import sys
import json
import logging

from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import script
from diadmin.dimockapi.mock_api import api

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

api.init(__file__)     # class instance of mock_api

# config parameter
#with open('../../../../http_connections/http_connection_usa.json') as fp:
with open('../../../../http_connections/http_connection.json') as fp:
    api.config.http_connection = json.load(fp)
print(api.config.http_connection)

# config parameter

#api.config.connection_id = "ECC_IDES_GCP"
#api.config.container = '/TABLES/SD/SD-SLS'
api.config.connection_id = 'HANA_Cloud_DQM'
api.config.container = '/QMGMT/BARCELONA_CSKB_PREP'
#api.config.container = '/QMGMT/CSKX_PT2'

script.gen()

for m in api.msg_list:
    if m['msg'] == "" :
        continue
    fsheet = json.loads(m['msg'])
    fname = fsheet["adaptedDataset"]["remoteObjectReference"]["name"]
    with open(join("../../../../catalogs/factsheets",fname+'.json'),'w') as fp:
        fp.write(m['msg'])