#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
########################################################
#### Using SAP Data Intelligence API Business Hub
#### Doc: https://api.sap.com/api/metadata/resource
########################################################
import logging
import json
from os import path
import re
from openapi_schema_validator import validate
from urllib.parse import urljoin

import yaml
import requests

from diadmin.utils.utils import get_system_id

### CONNECTIONS
def get_connections(connection,filter_type='',filter_tags=''):
    logging.info(f"Get Connections of type: {filter_type}")
    restapi = '/catalog/connections'
    url = connection['url'] + restapi
    parameter = {"connectionTypes":filter_type}
    headers = {'X-Requested-With': 'XMLHttpRequest'}

    r = requests.get(url, headers=headers, auth=connection['auth'], params=parameter)
    if r.status_code != 200:
        logging.error(r)
        return None

    resp = json.loads(r.text)
    if len(resp) == 0 :
        logging.info(f"No connections with filter type: {filter_type} - tags: {filter_tags} ")

    connections = dict()
    for c in resp :
        connections[c['id']] = {'id':c['id'],'type':c['type'],'description':c['description'],'tags':c['tags'],
                                'contentData':c['contentData']}
    return connections

def get_all_connections(connection):
    restapi = '/catalog/connections'
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}

    r = requests.get(url, headers=headers, auth=connection['auth'])

    if r.status_code != 200:
        logging.error(r)
        return None

    resp = json.loads(r.text)
    if len(resp) == 0 :
        logging.info(f"No connections")

    connections = dict()
    for c in resp.connections :
        connections[c['id']] = {'id':c['id'],'type':c['type'],'description':c['description'],'tags':c['tags'],
                                'contentData':c['contentData']}
    return connections

# ADD GRAPHDB
def add_connections_graphdb(gdb,connection,connections):
    node_tenant = {'label':'TENANT',
                   'properties':{'tenant':connection['TENANT'],'url':connection['URL']}}
    for k,c in connections.items() :
        node_connection = {'label':'CONNECTION',
                           'properties':{'id':c['id'],'type':c['type'],'description':c['description']},
                           'keys':['id']}
        gdb.create_node(node_connection)
        relationship = {'node_from':node_tenant,'node_to':node_connection,'relation':{'label':'HAS_CONNECTION'}}
        gdb.create_relationship(relationship)

def get_connection_details(connection,connection_id) :

    url = connection['url'] + f'/catalog/connections/{connection_id}'

    logging.info(f'Get connections: {url}')
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'])

    response = json.loads(r.text)
    if r.status_code != 200:
        logging.error(f"Connection details: {response['message']}  status: {r.status_code}\n{response}")

    return response

def get_nat_getway(connection) :
    #conn_id = "INFO_NAT_GATEWAY_IP"
    url = connection['url'] + f'/catalog/connections'
    logging.debug(f'Get INFO_NAT_GATEWAY_IP: {url}')
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {'connectionTypes':'HTTP'}
    r = requests.get(url, headers=headers, auth=connection['auth'],params=params)

    response = json.loads(r.text)
    if r.status_code != 200:
        logging.error(f"Check TaskID status: {response['message']}  status: {r.status_code}\n{response}")
        return None

    for c in response:
        if c['id'] == 'INFO_NAT_GATEWAY_IP' :
            return re.match(r".+(\d+\.\d+\.\d+\.\d+)",c['description']).group(1)

    logging.warning("'INFO_NAT_GATEWAY_IP' not found in connections!")
    return None

def upload_connection(connection, conn_data) :
    logging.info(f"Upload connection: {conn_data['id']}")
    url = connection['url'] + '/catalog/connections'
    headers = {'X-Requested-With': 'XMLHttpRequest','content-type':'application/json'}
    payload = json.dumps(conn_data)
    r = requests.post(url, headers=headers,auth=connection['auth'], data = payload)
    if r.status_code in [400,409,500]:
        logging.error(f"Upload Connection Error: {r.text}  status: {r.status_code}\n{r}")
        return None
    if r.status_code == 201 :
        logging.info(f"Connection successfully uploaded!")
        return None
    if r.status_code == 200 :
        response = json.loads(r.text)
        logging.info(f"Connection created, but open options: {response['message']}  status: {r.status_code}\n{response}")
        return None

def delete_connection(connection,connection_id):
    url = connection['url'] + f'/catalog/connections/{connection_id}'
    logging.debug(f'Delete connection: {connection_id}')
    headers = {'X-Requested-With': 'XMLHttpRequest','showMetadataImport':'true'}
    r = requests.delete(url, headers=headers,auth=connection['auth'])
    if r.status_code == 204:
        logging.info('Delete successfull')
    elif r.status_code == 404:
        logging.error(f'Connection {connection_id} does not exist')
    else:
        logging.error(f'Delete Connection {connection_id} error: {r.text}')

def upload_dataset(connection,data) :
    url = connection['url'] + f'/catalog/datasets'
    logging.debug(f'Upload datasets')
    headers = {'X-Requested-With': 'XMLHttpRequest','content-type':'application/json'}
    payload = json.dumps(data)
    r = requests.post(url, headers=headers,auth=connection['auth'], data = payload)
    if r.status_code == 202:
        logging.info("Datasets accepted for upload")
        return r.text
    if r.status_code == 400:
        logging.info(f"Schema error: {r.text}")
        return None
    if r.status_code in [403,404,500]:
        logging.info(f"Upload error: {r.text}")
        return None


def main() :
    logging.basicConfig(level=logging.INFO)

    with open('config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)


    api_path = '/app/datahub-app-metadata/api/v1'
    conn = {'url':params['URL']+api_path,
            'auth':(params['TENANT']+'\\'+ params['USER'],params['PWD'])}

    GET_CONNECTIONS = False
    if GET_CONNECTIONS :
        r = get_connections(conn)
        filename = get_system_id(params['URL'],params['TENANT']) + '_connections.json'
        with open(path.join('connections',filename),'w') as fp :
            json.dump(fp=fp,obj=r,indent=4)

    DELETE_CONNECTION = False
    if DELETE_CONNECTION :
        conns = get_connections(conn,connection_types='METADATA_IMPORT')
        for c in conns :
            b = input(f"\nDelete connection \'{c['id']}\' (Y/N):  ")
            if b == 'Y':
                delete_connection(conn,c['id'])
        print(json.dumps(get_connections(conn,connection_types='METADATA_IMPORT'),indent=4))

    GET_NAT_GATEWAY = False
    if GET_NAT_GATEWAY :
        ng = get_nat_getway(conn)
        print(f'NAT Gateway IP: {ng}')

    UPLOAD_CONNECTION = False
    if UPLOAD_CONNECTION:

        # load metadata_dataset
        with open(path.join('connections','couchdb_metadata_import.json')) as fp :
            conn_data = json.load(fp)

        # load dataset schema
        with open(path.join('connections','connection_schema.yaml')) as yamls:
            cschema = yaml.safe_load(yamls)
        validate(conn_data,cschema)

        upload_connection(conn,conn_data)
        print(json.dumps(get_connections(conn,connection_types='METADATA_IMPORT'),indent=4))
        delete_connection(conn,conn_data['id'])
        print(json.dumps(get_connections(conn,connection_types='METADATA_IMPORT'),indent=4))

    UPLOAD_DATASET = True
    if UPLOAD_DATASET :
        # load metadata_dataset
        with open(path.join('metadata_datasets','couchdb_visits.json')) as fp :
            conn_data = json.load(fp)

        # load dataset schema
        with open(path.join('metadata_datasets','dataset_schema.yaml')) as yamls:
            dschema = yaml.safe_load(yamls)
        validate(conn_data,dschema)

        task = upload_dataset(conn,conn_data)
        print(task)
        #print(json.dumps(conn_data,indent=4))



if __name__ == '__main__' :
    main()
