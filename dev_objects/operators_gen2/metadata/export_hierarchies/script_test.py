import sys
import json

from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import script
from diadmin.dimockapi.mock_api import api

api.init(__file__)     # class instance of mock_api

# config parameter
with open(join('http_connections','http_connection.json')) as fp:
    api.config.http_connection = json.load(fp)
print(api.config.http_connection)

# config parameter
api.config.hierarchy_name = None

script.gen()

for m in api.msg_list:
    print(m['msg'])