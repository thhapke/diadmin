# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import mock_api
#api = mock_api(__file__)

import os
import json
import requests
import http.client
from base64 import b64encode


def gen():



    #att = {'user':user,'tenant':tenant,'password':api.config.password}
    att = api.config.http_connection

    msg = api.Message(attributes=att,body = None )
    api.send('output',msg)  # data type: string

api.add_generator(gen)