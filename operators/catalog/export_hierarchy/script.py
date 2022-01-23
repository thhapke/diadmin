#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
# Mock apis needs to be commented before used within SAP Data Intelligence
from diadmin.dimockapi.mock_api import api
api = api(__file__)

from urllib.parse import urljoin
from diadmin.metadata_api import catalog

def gen():

    host = api.config.http_connection['connectionProperties']['host']
    pwd = api.config.http_connection['connectionProperties']['password']
    user = api.config.http_connection['connectionProperties']['user']
    path = api.config.http_connection['connectionProperties']['path']
    tenant = api.config.tenant

    conn = {'url':urljoin(host,path),'auth':(tenant+'\\'+ user,pwd)}

    api.logger.info(f"Download hierarchies:  {api.config.hierarchies}")
    hierarchies = catalog.download_hierarchies(conn,hierarchy_name=api.config.hierarchies)

    hierarchies_name = 'hierarchies' if not api.config.hierarchies else api.config.hierarchies
    att = {'hierarchies':hierarchies_name}
    msg = api.Message(attributes=att,body=hierarchies)
    api.send('output',msg)  # dev_data type: message

api.add_generator(gen)