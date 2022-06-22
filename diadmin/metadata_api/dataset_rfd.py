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
from urllib.parse import urljoin
import urllib
from os import path

import yaml
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF

from diadmin.utils.utils import get_system_id
from diadmin.metadata_api.connection import get_connections
from diadmin.metadata_api.container import get_containers, get_datasets

from diadmin.dimockapi.mock_api import api

import os
from urllib.parse import urljoin
import urllib
import json

import requests


#
#  GET Datasets
#
def get_datasets(connection, connection_id, dataset_path):
    qualified_name = urllib.parse.quote(dataset_path, safe='')  # quote to use as  URL component
    restapi = f"/catalog/connections/{connection_id}/containers/{qualified_name}"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'])

    if r.status_code != 200:
        api.logger.error(f"Status code: {r.status_code}  - {r.text}")
        return -1

    return json.loads(r.text)['datasets']


#
#  GET Tags of datasets
#
def get_dataset_factsheets(connection,  connection_id, dataset_path):
    qualified_name = urllib.parse.quote(dataset_path, safe='')  # quote to use as  URL component
    restapi = f"/catalog/connections/{connection_id}/datasets/{qualified_name}/factsheet"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {"connectionId": connection_id, "qualifiedName": dataset_path}
    r = requests.get(url, headers=headers, auth=connection['auth'], params=params)

    if r.status_code != 200:
        api.logger.error(f"Status code: {r.status_code}  - {r.text}")
        return -1
    return json.loads(r.text)


#
#  GET Tags of datasets
#
def get_dataset_tags(connection, connection_id, dataset_path):
    qualified_name = urllib.parse.quote(dataset_path, safe='')  # quote to use as  URL component
    restapi = f"/catalog/connections/{connection_id}/datasets/{qualified_name}/tags"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {"connectionId": connection_id, "qualifiedName": dataset_path}
    r = requests.get(url, headers=headers, auth=connection['auth'], params=params)

    if r.status_code != 200:
        api.logger.error(f"Status code: {r.status_code}  - {r.text}")
        return -1
    return json.loads(r.text)


#
#  GET Lineage of datasets
#
def get_dataset_lineage(connection, connection_id, dataset_path):
    qualified_name = urllib.parse.quote(dataset_path, safe='')  # quote to use as  URL component
    restapi = f"/catalog/lineage/export"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {"connectionId": connection_id, "qualifiedNameFilter": dataset_path}
    r = requests.get(url, headers=headers, auth=connection['auth'], params=params)

    if r.status_code == 404:
        api.logger.error(f"Status code: {r.status_code}  - No lineage found for: {dataset_path}")
        return -1
    if r.status_code == 500:
        api.logger.error(f"Status code: {r.status_code}  - {r.text}")
        return -1
    return json.loads(r.text)


#
# Callback of operator
#
def main():
    logging.basicConfig(level=logging.INFO)

    with open('config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    connection = {'url': params['URL'] + '/app/datahub-app-metadata/api/v1',
                  'host': params['URL'], 'tenant': params['TENANT'],
                  'auth': (params['TENANT'] + '\\' + params['USER'], params['PWD'])}

    # Config parameters
    connection_id = 'HANA_Cloud_DQM'
    container_path = '/QMGMT'
    tags = True
    lineage = True

    # Get all catalog datasets under container path
    # api.logger.info(f"Get datasets: {connection_id} - {container_path}")
    logging.info(f"Get datasets: {connection_id} - {container_path}")
    datasets = get_datasets(connection, connection_id, container_path)

    dataset_factsheets = list()
    for i, ds in enumerate(datasets):
        qualified_name = ds['remoteObjectReference']['qualifiedName']
        # api.logger.info(f'Get dataset metadata: {qualified_name}')
        logging.info(f'Get dataset metadata: {qualified_name}')
        dataset = get_dataset_factsheets(connection, connection_id, qualified_name)

        # In case of Error (like imported data)
        if dataset == -1:
            continue

        if tags:
            dataset['tags'] = get_dataset_tags(connection, connection_id, qualified_name)

        if lineage:
            dataset['lineage'] = get_dataset_lineage(connection, connection_id, qualified_name)

        dataset_factsheets.append(dataset)


    header = [0, True, 1, 0, ""]
    header = {"com.sap.headers.batch": header}
    datasets_json = json.dumps(dataset_factsheets, indent=4)
    api.outputs.datasets.publish(datasets_json, header=header)


def export(connection, g=None):

    # RDF Graph
    if not g:
        g = Graph()

    # Name spaces
    ndi = Namespace('https://www.sap.com/products/data-intelligence#')
    g.namespace_manager.bind('di', URIRef(ndi))

    nsys = Namespace(connection['host']+'/' + connection['tenant']+'/')
    g.namespace_manager.bind('instance', URIRef(nsys))

    # CONNECTIONS
    connection_filter = 'HANA_DB'
    connections = get_connections(connection, filter_type=connection_filter)
    for c in connections:
    # g.add((nsys[qname], ndi.belongsConnection, nsys[c['connectionId']]))
        pass
    # CONTAINER
    containers = dict()
    container_filter = f"connectionType eq '{connection_filter}'"
    get_containers(connection, containers, container_filter=container_filter)
    for c in containers.values():
        # Connection level
        if c['parentId'] == 'connectionRoot':
            g.add((nsys[c['connectionId']], RDF.type, ndi.Connection))
            continue
        qname = urllib.parse.quote(c['qualifiedName'], safe='')
        p_qname = urllib.parse.quote(c['parentQualifiedName'], safe='')
        g.add((nsys[qname], RDF.type, ndi.Container))
        g.add((nsys[qname], ndi.hasId, Literal(c['id'])))
        g.add((nsys[qname], ndi.hasName, Literal(c['name'])))
        g.add((nsys[qname], ndi.belongsConnection, nsys[c['connectionId']]))

        if c['name'] == c['qualifiedName']:
            g.add((nsys[c['connectionId']], ndi.hasContainer,nsys[qname]))
            g.add((nsys[qname], ndi.isChild, nsys[c['connectionId']]))
        else:
            g.add((nsys[qname], ndi.isChild, nsys[p_qname]))
            g.add((nsys[p_qname], ndi.hasChild, nsys[qname]))

        # DATASETS
        for name, container in containers.items():
            datasets = get_datasets(connection, container['connectionId'], container['name'])
            for dataset in datasets:
                pass
            '''
            add_dataset_graphdb(gdb,dataset)
            # ATTRIBUTES
            ds = get_dataset_summary(connection, dataset['qualifiedName'], dataset['id'])
            if 'code' in ds and ds['code'] == '70016' :
                logging.warning(f"Read dataset summary for {dataset['qualifiedName']}: {ds['message']}")
                continue
            for att in ds['additionalInfo']['columns']:
                add_dataset_attribute_graphdb(gdb,dataset['id'],att)

            # TAGS
            dataset_tags = get_dataset_tags(connection, dataset['qualifiedName'], dataset['connectionId'])
            add_tag_relationship_graphdb(gdb,dataset['id'],dataset_tags)
            '''
    return g

#########
# MAIN
########


def main():

    logging.basicConfig(level=logging.INFO)

    with open('config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    connection = {'url': params['URL'] + '/app/datahub-app-metadata/api/v1',
                  'host': params['URL'], 'tenant': params['TENANT'],
                  'auth': (params['TENANT']+'\\' + params['USER'], params['PWD'])}

    sysid = get_system_id(params['URL'], params['TENANT'])

    g = export(connection)
    g.serialize(destination=path.join('metadata_graph', 'datasets.ttl'))


if __name__ == '__main__':
    main()
