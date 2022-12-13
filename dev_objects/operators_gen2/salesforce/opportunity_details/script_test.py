import sys
import json

from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import script
from diadmin.dimockapi.mock_api import api

api.init(__file__)     # class instance of mock_api


api.config.httpconnection_basic = { 'connectionProperties':
                           {'host': 'https://api-staging.pwc.com/',
                            'user': 'DE_ifs_Integration-Platform_s001',
                            'password': 'D0uV%/r(I^!_>Q}gK1Eo',
                            'path': '/de-ip_temp'
                            }
                       }

api.config.httpconnection_details = { 'connectionProperties':
                           {'host': 'https://api-staging.pwc.com/',
                            'user': '3d05882e-6f47-40b6-a1f5-dc6ff6197dd0 0832861272c45caee336459ff49cd582 '\
                            'l712dedd2e8d504e21ab1f2e89988a64ab aba40940b5244c1ebc1e02f7c176ae26',
                            'password': 'Basic REVfaWZzX0ludGVncmF0aW9uLVBsYXRmb3JtX3MwMDE6RDB1ViUvcihJXiFfPlF9Z0sxRW8=',
                            'path': '/de-ip_temp'
                            }
                       }

api.config.territory = 'DE'  # type: string

print("Call on_input")
script.on_input(msg_id=1, header=None, data='0063G000003iD2DQAU')


