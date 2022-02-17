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

import requests
import json
import yaml


def get_connections(connection,filter_type='',filter_tags=''):
    logging.info(f"Get Connections of type: {filter_type}")
    restapi = '/catalog/connections'
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    parameter = {"connectionTypes":filter_type}
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

def get_containers(connection,containers,container, container_filter=None):

    # exclude internal DI_Data_Lake
    if re.match('\/*DI_DATA_LAKE',container['qualifiedName']) :
        return 1

    if not container['id'] == 'connectionRoot' :
        container['qualifiedName'] = container['qualifiedName'].strip('/')
        container["parentQualifiedName"] = container['parentQualifiedName'].strip('/')
        containers[container['qualifiedName']] = container

    logging.info(f"Get sub-container of {container['name']}")
    restapi = f"/catalog/containers/{container['id']}/children"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {'containerId': container['id'], 'type':'CONTAINER','$filter': container_filter}
    r = requests.get(url, headers=headers, auth=connection['auth'], params=params)

    if r.status_code != 200:
        logging.error(r.status)
        return -1

    sub_containers = json.loads(r.text)['containers']
    for c in sub_containers:
        get_containers(connection,containers,c,container_filter=container_filter)
    return containers


def get_datasets(connection, container_id):
    restapi = f"/catalog/containers/{container_id}/children"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {"containerId": container_id, "type": "DATASET"}
    r = requests.get(url, headers=headers, auth=connection['auth'], params=params)

    response = json.loads(r.text)
    if r.status_code != 200:
        logging.error("Get datasets: {}".format(response['message']))
    return response

def get_dataset_tags(connection,dataset_path,tag_hierarchies=None):
    path_list = dataset_path.strip('/').split("/")
    connection_id = path_list[0]
    qualified_name = '/'+'/'.join(path_list[1:])
    qualified_name = urllib.parse.quote(qualified_name,safe='')
    restapi = f"/catalog/connections/{connection_id}/datasets/{qualified_name}/tags"
    url = connection['url'] + restapi
    logging.info(f'get_dataset_tags URL: {url}')
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {"connectionId": connection_id, "qualifiedName": dataset_path,'tagHierarchies':tag_hierarchies}
    r = requests.get(url, headers=headers, auth=connection['auth'], params=params)

    if r.status_code != 200:
        logging.error(f"Get datasets attributes unsuccessful for {dataset_path}: {r.status}\n{json.loads(r.text)}")
        return None

    return json.loads(r.text)

# Add tag to dataset
def add_dataset_tag(connection,dataset_path,hierarchy_id,tag_id) :

    path_list = dataset_path.strip('/').split("/")
    connection_id = path_list[0]
    qualified_name = '/'+'/'.join(path_list[1:])
    qualified_name = urllib.parse.quote(qualified_name,safe='')
    restapi = f"/catalog/connections/{connection_id}/datasets/{qualified_name}/tagHierarchies/{hierarchy_id}/tags"
    url = connection['url'] + restapi
    logging.info(f'get_dataset_tags URL: {url}')
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    data = {"tagId":tag_id}
    r = requests.post(url, headers=headers, auth=connection['auth'], data = data)

    response = json.loads(r.text)
    if r.status_code != 201:
        logging.error(f"Adding tag to dataset: {response['message']}  status: {r.status_code}\n{response}")

    return response

# Add tag to dataset
def add_dataset_attribute_tag(connection,dataset_path,hierarchy_id,tag_id,attributes_qn) :

    path_list = dataset_path.strip('/').split("/")
    connection_id = path_list[0]
    qualified_name = '/'+'/'.join(path_list[1:])
    qualified_name = urllib.parse.quote(qualified_name,safe='')
    restapi = f"/catalog/connections/{connection_id}/datasets/{qualified_name}/attributes/{attributes_qn}/tagHierarchies/{hierarchy_id}/tags"
    url = connection['url'] + restapi
    logging.info(f'add_dataset_attribute_tag URL: {url}')
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    data = {"tagId":tag_id}
    r = requests.post(url, headers=headers, auth=connection['auth'], data = data)

    response = json.loads(r.text)
    if r.status_code != 201:
        logging.error(f"Adding tag to dataset: {response['message']}  status: {r.status_code}\n{response}")

    return response

def reduce_dataset_attributes(dtags):
    tags = {'dataset_tags':list(),'attribute_tags':dict()}
    for t in dtags['tagsOnDataset'] :
        for tt in t['tags'] :
            tag_path = t['hierarchyName'] + '/' + tt['tag']['path'].replace('.','/')
            tags['dataset_tags'].append(tag_path)
    for at in dtags['tagsOnAttribute']:
        for atn in at['tags'] :
            for attn in atn['tags'] :
                tag_path = atn['hierarchyName'] + '/' + attn['tag']['path'].replace('.','/')
                tags['attribute_tags'][at['attributeQualifiedName']] = tag_path
    return tags

#########
# MAIN
########
if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    with open('config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    conn = {'url': urljoin(params['URL'] , '/app/datahub-app-metadata/api/v1'),
            'auth': (params['TENANT'] + '\\' + params['USER'], params['PWD'])}


    CONNECTIONS = False
    if CONNECTIONS :
        #connection_type = "SDL"
        connection_type = ''
        connections = get_connections(conn,filter_type=connection_type)
        with open(path.join('catalogs','connections.json'),'w') as fp:
            json.dump(connections,fp,indent=4)


    CONTAINER_CALL = False
    if CONTAINER_CALL :
        root = {'id': 'connectionRoot',
                     'name': 'Root',
                     'qualifiedName': '/',
                     'catalogObjectType': 'ROOT_FOLDER'}

        containers = dict()
        container_filter = 'connectionType eq \'SDL\''
        get_containers(conn,containers,root,container_filter=None)

        with open(path.join('catalogs','containers.json'),'w') as fp:
            json.dump(containers,fp,indent=4)

    GET_ATTRIBUTES = False
    if GET_ATTRIBUTES :

        #idc = containers['S3_Catalog/costcenter']['id']
        #datasets = get_datasets(conn,idc)
        with open(path.join('catalogs','containers.json'),'r') as fp:
            containers = json.load(fp)

        tags = dict()
        dataset_qn = 'S3_Catalog/costcenter/BARCELONA_CSKA.csv'
        dataset_attributes = get_dataset_tags(conn,dataset_qn)
        tags[dataset_qn] = reduce_dataset_attributes(dataset_attributes)
        with open(path.join('catalogs','dataset_tags.json'),'w') as fp:
            json.dump(tags,fp,indent=4)

    ADD_ATTRIBUTES = True
    if ADD_ATTRIBUTES :
        with open(path.join('catalogs','dataset_tags.json'),'r') as fp:
            dataset_tags = json.load(fp)

        with open(path.join('catalogs','hierarchies.json'),'r') as fp:
            hierarchies = json.load(fp)

        for ds,dsv in dataset_tags.items():
            for dst in dsv['dataset_tags']:
                hierarchy_id = hierarchies[dst]['hierarchy_id']
                tag_id = hierarchies[dst]['tag_id']
                add_dataset_tag(conn,ds,hierarchy_id,tag_id)
            for attribute, ast in dsv['attribute_tags'].items():
                hierarchy_id = hierarchies[ast]['hierarchy_id']
                tag_id = hierarchies[ast]['tag_id']
                add_dataset_attribute_tag(conn,ds,hierarchy_id,tag_id,attribute)
