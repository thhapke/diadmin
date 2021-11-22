# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import mock_api
#api = mock_api(__file__)

import json
from urllib.parse import urljoin

from diadmin.metadata_api import catalog

def gen():

    host = api.config.http_connection['connectionProperties']['host']
    pwd = api.config.http_connection['connectionProperties']['password']
    user = api.config.http_connection['connectionProperties']['user']
    path = api.config.http_connection['connectionProperties']['path']
    tenant = api.config.tenant

    conn = {'url':urljoin(host,path),'auth':(tenant+'\\'+ user,pwd)}

    hnames = catalog.get_hierarchy_names(conn, search=api.config.hierarchy)

    if hnames :
        hierarchy = catalog.get_hierarchy_tags(conn, hnames['tagHierarchies'][0]["tagHierarchy"]['id'])
        if api.config.convert :
            hierarchy = catalog.convert_standard_hierarchy(hierarchy)
    else :
        raise ValueError(f"Hierarchy not found: {api.config.hierarchy}")

    att = {'hierarchy':api.config.hierarchy}
    msg = api.Message(attributes=att,body=hierarchy)
    api.send('output',msg)  # data type: message

api.add_generator(gen)