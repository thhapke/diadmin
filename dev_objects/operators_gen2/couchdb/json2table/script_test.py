import sys
import json

from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import script
from diadmin.dimockapi.mock_api import api
import logging

api.init(__file__)     # class instance of mock_api

# config parameter
api.config.errorHandling = '{"type":"terminate on error"}' # type: string
logging.basicConfig(level=logging.INFO)

with open(join('tmp', 'couchdb.json')) as fp:
    db_datat = fp.read()
data = api.Message(db_datat)

header = ['host', 'db', 'view', 'api.config.startkey', 'api.config.endkey', 0, 0]
header = {"diadmin.couchdb.header": header}
script.on_input(0, header, data)

#for m in api.msg_list:
#    print(m['msg'])

