import sys
import json

from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import script
from diadmin.dimockapi.mock_api import api

api.init(__file__)     # class instance of mock_api

# config parameter
api.config.errorHandling = '{"type":"terminate on error"}'  # type: string

#https://mcs.gauselmann.com - statistics - checkIn_2015 - /statistics/_design/sap_analytics/_view

api.config.couchdb = { 'connectionProperties':
                           {'host' : 'https://mcs.gauselmann.com:5984',
                            'user' : 'statistics',
                            'password': 'checkIn_2015',
                            'path':'/statistics/_design/sap_analytics/_view'
                            }
                       }

api.config.startkey = '2022/01/01'  # type: string
api.config.endkey = '2022/01/04'  # type: string
api.config.view = 'visits'  # type: string


script.gen()


result = json.loads(api.msg_list[0]['msg'])
with open('tmp/couchdb.json', 'w') as fp:
    json.dump(result, fp, indent=4)


