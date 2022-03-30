import sys
import json
from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import script
from diadmin.dimockapi.mock_api import api


# config parameter
with open('http_connection2.json') as fp:
    api.config.http_connection = json.load(fp)
print(api.config.http_connection)

api.config.connection_id = 'ECC_IDES_GCP'
api.config.container = '{"name":"SAP-R/3","qualifiedName":"/ODP_SAPI/SAP/SAP-R/3","parentQualifiedName":"/ODP_SAPI/SAP"}'


script.gen()
