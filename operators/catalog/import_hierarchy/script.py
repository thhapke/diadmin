# Mock apis needs to be commented before used within SAP Data Intelligence
from diadmin.dimockapi.mock_api import mock_api
api = mock_api(__file__)

import copy
import io
import json
from urllib.parse import urljoin

from diadmin.metadata_api import catalog

def on_input(msg):

    host = api.config.http_connection['connectionProperties']['host']
    pwd = api.config.http_connection['connectionProperties']['password']
    user = api.config.http_connection['connectionProperties']['user']
    path = api.config.http_connection['connectionProperties']['path']
    tenant = api.config.tenant

    conn = {'url':urljoin(host,path),'auth':(tenant+'\\'+ user,pwd)}

    new_hierarchies = json.load(io.BytesIO(msg.body))
    hierarchies = catalog.upload_hierarchies(conn,new_hierarchies,hierarchies=None)

    att = copy.deepcopy(msg.attributes)
    msg_success = api.Message(attributes=att,body=hierarchies)
    api.send('success',msg_success)  # dev_data type: message


api.set_port_callback('input',on_input)