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

import requests
import json
import yaml

from diadmin.utils.utils import get_system_id
from diadmin.metadata_api.container import get_containers, get_datasets_container, get_dataset_summary, get_ids
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

def get_catalog_lineates(connection):
    datasets_id = dict()
    get_ids(connection, datasets_id)
    get_containers(connection, containers)
    for name,container in containers.items() :
        datasets = get_datasets_container(connection, container['id'])
        for dataset in datasets :
            dsummary = get_dataset_summary(connection, dataset['qualifiedName'], dataset['id'])
            if dsummary['hasLineage']:
                lineage = get_lineage(connection, dataset['qualifiedName'], dataset['id'])
                print(json.dumps(lineage,indent=4))

def add_lineage(gdb,lineage) :
    key_nodes = dict()
    for n in lineage['nodes']:
        if not 'transform' in n:
            sup_ref = n['dataset']['remoteReferences'][0]
            if 'publishedDataset' in n['dataset']['remoteReferences'][0]:
                key_nodes[n['key']] = {'label':'DATASET','properties':{'id':sup_ref['publishedDataset']['datasetId']}}
            else :
                key_nodes[n['key']] = {'label':'DATASET','properties':{'connection_id':sup_ref['connectionId'],
                                                                      'path':sup_ref['qualifiedName']}}
        else :
            key_nodes[n['key']] = {'label':'TRANSFORM','properties':{'id':n['transform']['definition']['computationArtifactRef'],
                                                                         'name':n['transform']['definition']['name'],
                                                                         'type':n['transform']['transformType']}}
            gdb.create_node(key_nodes[n['key']])

    nkeys = { n['key']:len(n['dataset']['remoteReferences']) for n in lineage['nodes'] if not 'transform' in n}
    for k,n in nkeys.items():
        if n > 1 :
            logging.warning(f"Lineage of lineage key \'{k}\' has {n} references")

    for edge in lineage['edges']:
        logging.info(f"Lineage relationship")
        relationship = {'node_from':key_nodes[edge['from']],'node_to':key_nodes[edge['to']],'relation':{'label':edge['kind']}}
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
        datasets = get_datasets_container(connection, container['id'])
        for dataset in datasets :
            dsummary = get_dataset_summary(connection, dataset['qualifiedName'], dataset['id'])
            if dsummary['hasLineage']:
                lineage = get_lineage(connection, dataset['qualifiedName'], dataset['id'])
                print(json.dumps(lineage,indent=4))
                #add_lineage(gdb,lineage)