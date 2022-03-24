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
import sys
from urllib.parse import urljoin
import urllib
from os import path
import re
import csv


import requests
import json
import yaml



from diadmin.utils.utils import get_system_id
from diadmin.metadata_api.container import get_connections, get_containers, get_datasets, get_dataset_summary
from diadmin.connect.connect_neo4j import neo4jConnection


def get_lineage(connection,dataset_name,dataset_id) :
    logging.info(f"Get Dataset Lineage: {dataset_name}")
    restapi = f'catalog/datasets/{dataset_id}/lineage'
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'])

    response = json.loads(r.text)
    if r.status_code != 200:
        logging.error("Get lineage: {}".format(response['message']))
    return response

def add_lineage(gdb,lineage) :
    keys = dict()
    for n in lineage['nodes']:
        if 'transform' in n:
            continue
        elif 'publishedDataset' in n['dataset']['remoteReferences'][0]:
            keys['key'] = n['dataset']['remoteReferences'][0]['publishedDataset']['datasetId']
        else :
            keys['key'] = (n['dataset']['remoteReferences'][0]['connectionId'],n['dataset']['remoteReferences'][0]['qualifiedName'])
    nkeys = { n['key']:len(n['dataset']['remoteReferences']) for n in lineage['nodes'] if not 'transform' in n}
    for k,n in nkeys.items():
        if n > 1 :
            logging.warning(f"Lineage of lineage key \'{k}\' has {n} references")
    for edge in lineage['edges']:
        if not edge['from'] in keys or not edge['to'] in keys:
            continue
        if isinstance(keys[edge['from']],tuple) :
            node_from = {'label':'DATASET','properties':{'connection_id':keys[edge['from']][0],
                                                         'path':keys[edge['from']][1]}}
        else :
            node_from = {'label':'DATASET','properties':{'id':keys[edge['from']]}}
        if isinstance(keys[edge['to']],tuple) :
            node_to = {'label':'DATASET','properties':{'connection_id':keys[edge['to']][0],
                                                         'path':keys[edge['to']][1]}}
        else :
            node_to = {'label':'DATASET','properties':{'id':keys[edge['to']]}}

        relationship = {'node_from':node_from,'node_to':node_to,'relation':{'label':'LINEAGE_'+edge['kind']}}
        gdb.create_relationship(relationship)



#########
# MAIN
########
if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    with open('config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    connection = {'url': urljoin(params['URL'], '/app/datahub-app-metadata/api/v1'),
                  'auth': (params['TENANT'] + '\\' + params['USER'], params['PWD'])}

    if 'GRAPHDB' in params:
        connection['GRAPHDB'] = params['GRAPHDB']
        connection['TENANT'] = params['TENANT']
        connection['URL'] = params['URL']


    sysid = get_system_id(params['URL'],params['TENANT'])

    gdb = neo4jConnection( connection['GRAPHDB']['URL']+':'+str(connection['GRAPHDB']['PORT']), \
                           connection['GRAPHDB']['USER'], connection['GRAPHDB']['PWD'],connection['GRAPHDB']['DB'])

    containers = dict()
    get_containers(connection, containers)
    for name,container in containers.items() :
        datasets = get_datasets(connection, container['id'])
        for dataset in datasets :
            dsummary = get_dataset_summary(connection, dataset['qualifiedName'], dataset['id'])
            if dsummary['hasLineage']:
                lineage = get_lineage(connection, dataset['qualifiedName'], dataset['id'])
                add_lineage(gdb,lineage)