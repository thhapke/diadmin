import sys
import json

from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import script
from diadmin.dimockapi.mock_api import api

api.init(__file__)     # class instance of mock_api


api.config.httpconnection_basic = { 'connectionProperties':
                           {'host': '',
                            'user': '',
                            'password': '',
                            'path': 'p'
                            }
                       }

api.config.httpconnection_details = { 'connectionProperties':
                           {'host': 'https://api-staging.pwc.com/',
                            'user': '',
                            'password': '',
                            'path': '/de-ip_temp'
                            }
                       }

api.config.territory = 'DE'  # type: string

print("Call on_input")
script.on_input(msg_id=1, header=None, data='0063G000003iD2DQAU')


