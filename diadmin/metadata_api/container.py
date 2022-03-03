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
import pandas as pd


from diadmin.utils.utils import get_system_id
from diadmin.metadata_api.catalog import download_hierarchies


def re_str(pattern) :
    if pattern[0]=='*' and pattern[-1]=='*' :
        re_pat = f'.*{pattern[1:-1]}.*'
    elif pattern[0]=='*':
        re_pat = f'.*{pattern[1:]}$'
    elif pattern[-1]=='*':
        re_pat = f'^{pattern[:-1]}.+'
    else:
        re_pat = f'^{pattern}$'
    return re_pat


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

def get_containers(connection,containers,container = None,container_filter=None):

    if container == None :
        container = {'id': 'connectionRoot',
                     'name': 'Root',
                     'qualifiedName': '/',
                     'catalogObjectType': 'ROOT_FOLDER'}

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

    return response['datasets']

def get_dataset_summary(connection,dataset_name,dataset_id):
    logging.info(f"Get Dataset summary: {dataset_name}")
    restapi = f'/catalog/datasets/{dataset_id}/summary'
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'])

    response = json.loads(r.text)
    if r.status_code != 200:
        logging.error("Get datasets: {}".format(response['message']))
    return response

def get_dataset_tags(connection,dataset_path,connection_id=None,tag_hierarchies=None):
    path_list = dataset_path.strip('/').split("/")
    if connection_id == None or connection_id == path_list[0]:
        connection_id = path_list[0]
        qualified_name = '/'+'/'.join(path_list[1:])
    else :
        qualified_name = '/'+'/'.join(path_list[0:])

    qualified_name = urllib.parse.quote(qualified_name,safe='')
    restapi = f"/catalog/connections/{connection_id}/datasets/{qualified_name}/tags"
    url = connection['url'] + restapi
    logging.info(f'Get Dataset Tags for: {dataset_path} ({connection_id})')
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {"connectionId": connection_id, "qualifiedName": dataset_path,'tagHierarchies':tag_hierarchies}
    r = requests.get(url, headers=headers, auth=connection['auth'], params=params)

    if r.status_code != 200:
        logging.error(f"Get datasets attributes unsuccessful for {dataset_path}: {r.status}\n{json.loads(r.text)}")
        return None
    return json.loads(r.text)

def export_catalog(connection) :
    containers = dict()
    datasets = list()
    datasets_cols = list()
    containers = get_containers(connection,containers)
    for name,container in containers.items() :
        dsets = get_datasets(connection, container['id'])
        if len(dsets) > 0:
            logging.info(f"Scanned container:{container['qualifiedName']} - #datasets:{len(dsets)}")
            datasets.extend(dsets)

    for dataset in datasets:
        ds = get_dataset_summary(connection,dataset['qualifiedName'],dataset['id'])
        if 'code' in ds and ds['code'] == '70016' :
            logging.warning(f"Read dataset summary for {dataset['qualifiedName']}: {ds['message']}")
            continue
        for c in ds['additionalInfo']['columns']:
            length = c['length'] if 'length' in c else ''
            dtype = '' if not 'templateType' in c else c['templateType']
            datasets_cols.append({'connection_type':ds['remoteObject']['connectionType'],
                                  'connection_id':ds['remoteObject']['connectionId'],
                                  'dataset_path':ds['remoteObject']['qualifiedName'],
                                  'dataset_name':ds['remoteObject']['name'],
                                  'column_name': c['name'],
                                  'length': length,
                                  'dtype':dtype})

    df = pd.DataFrame(datasets_cols)
    df_ds = df.groupby(by=['connection_id','dataset_path'])['connection_type'].first().reset_index()

    for i,dsc in df_ds.iterrows() :
        dataset_attributes = get_dataset_tags(connection,dsc['dataset_path'],dsc['connection_id'])
        tags = dataset_attributes_str(dataset_attributes)
        df.loc[(df['connection_id'] == dsc['connection_id']) & (df['dataset_path'] == dsc['dataset_path']),'dataset_tags'] = tags['dataset_tags']
        for at,at_tags in tags['attribute_tags'].items() :
            df.loc[(df['connection_id'] == dsc['connection_id']) &
                   (df['dataset_path'] == dsc['dataset_path']) &
                   (df['column_name'] == at),'column_tags'] = at_tags

    return df

# Add tag to dataset
def add_dataset_tag(connection,dataset_path,hierarchy_id,tag_id,connection_id=None) :

    path_list = dataset_path.strip('/').split("/")
    if connection_id == None or connection_id == path_list[0]:
        connection_id = path_list[0]
        qualified_name = '/'+'/'.join(path_list[1:])
    else :
        qualified_name = '/'+'/'.join(path_list[0:])

    qualified_name = urllib.parse.quote(qualified_name,safe='')
    restapi = f"/catalog/connections/{connection_id}/datasets/{qualified_name}/tagHierarchies/{hierarchy_id}/tags"
    url = connection['url'] + restapi
    logging.info(f'add_dataset_tags URL: {url}')
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    data = {"tagId":tag_id}
    r = requests.post(url, headers=headers, auth=connection['auth'], data = data)

    response = json.loads(r.text)
    if r.status_code != 201:
        logging.error(f"Adding tag to dataset: {response['message']}  status: {r.status_code}\n{response}")

    return response

# Add tag to dataset
def add_dataset_attribute_tag(connection,dataset_path,hierarchy_id,tag_id,attributes_qn,connection_id=None) :

    path_list = dataset_path.strip('/').split("/")
    if connection_id == None or connection_id == path_list[0]:
        connection_id = path_list[0]
        qualified_name = '/'+'/'.join(path_list[1:])
    else :
        qualified_name = '/'+'/'.join(path_list[0:])

    qualified_name = urllib.parse.quote(qualified_name,safe='')
    attributes_qn = urllib.parse.quote(attributes_qn,safe='')
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
    return 1

def dataset_attributes_str(dtags):
    tags = {'dataset_tags':'','attribute_tags':dict()}
    for t in dtags['tagsOnDataset'] :
        for tt in t['tags'] :
            tag_path = t['hierarchyName'] + '/' + tt['tag']['path'].replace('.','/')
            tags['dataset_tags'] +=  tag_path + ','
    if len(tags['dataset_tags']) > 0 :# remove last comma
        tags['dataset_tags'] = tags['dataset_tags'][:-1]
    for at in dtags['tagsOnAttribute']:
        tags['attribute_tags'][at['attributeQualifiedName']] = ''
        for atn in at['tags'] :
            for attn in atn['tags'] :
                tag_path = atn['hierarchyName'] + '/' + attn['tag']['path'].replace('.','/')
                tags['attribute_tags'][at['attributeQualifiedName']] += tag_path +","
        if len(tags['attribute_tags'][at['attributeQualifiedName']]) >0 :# remove last comma
            tags['attribute_tags'][at['attributeQualifiedName']] = tags['attribute_tags'][at['attributeQualifiedName']][:-1]
    return tags

def auto_tag(connection,metadata,tagging_map):

    tagmap = dict()
    tagmap['A'] =[{'pattern':re_str(t[1].lower()),'dtype':t[2],'length':t[3],'tag':t[4]} for t in tagging_map if t[0]=='A']
    tagmap['D'] =[{'pattern':re_str(t[1].lower()),'dtype':t[2],'length':t[3],'tag':t[4]} for t in tagging_map if t[0]=='D']

    hierarchies = download_hierarchies(connection, hierarchy_name=None)
    df = metadata
    df['normalized'] = df['column_name'].str.lower()
    df['new_column_tags'] = ''
    df['new_dataset_tags'] = ''

    ####### Colum attributes
    for t in tagmap['A']:
        df.loc[df['normalized'].str.match(t['pattern'])==True,'new_column_tags'] += ','+t['tag']

    maska = df['new_column_tags'].str.len() >0
    df.loc[maska,'new_column_tags'] += ','+'TagWorkflow/auto-tag'
    df.loc[maska,'new_column_tags'] = df.loc[maska,'new_column_tags'].str[1:]

    for i,dsc in df.loc[maska].iterrows():
        tags = dsc['new_column_tags'].split(',')
        for t in tags :
            add_dataset_attribute_tag(connection, dataset_path=dsc['dataset_path'],
                                      hierarchy_id=hierarchies[t]['hierarchy_id'],
                                      tag_id=hierarchies[t]['tag_id'],
                                      attributes_qn=dsc['column_name'], connection_id=dsc['connection_id'])


    ####### Dataset attributes
    df = df[['connection_type','connection_id','dataset_path','dataset_name','normalized','new_dataset_tags']].groupby(by=['connection_id','dataset_path']).first().reset_index()
    df['normalized'] = df['dataset_name'].str.lower()
    for t in tagmap['D']:
        df.loc[df['normalized'].str.match(t['pattern'])==True,'new_dataset_tags'] += ','+t['tag']

    maskd = df['new_dataset_tags'].str.len() >0
    df.loc[maskd,'new_dataset_tags'] += ','+'TagWorkflow/auto-tag'
    df.loc[maskd,'new_dataset_tags'] = df.loc[maskd,'new_dataset_tags'].str[1:]

    for i,dsc in df.loc[maskd].iterrows():
        tags = dsc['new_dataset_tags'].split(',')
        for t in tags :
            add_dataset_tag(connection, dataset_path=dsc['dataset_path'],
                            hierarchy_id=hierarchies[t]['hierarchy_id'],
                            tag_id=hierarchies[t]['tag_id'],
                            connection_id=dsc['connection_id'])





#########
# MAIN
########
if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    with open('config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    connection = {'url': urljoin(params['URL'], '/app/datahub-app-metadata/api/v1'),
            'auth': (params['TENANT'] + '\\' + params['USER'], params['PWD'])}

    sysid = get_system_id(params['URL'],params['TENANT'])

    CONNECTIONS = False
    if CONNECTIONS :
        #connection_type = "SDL"
        connection_type = ''
        connections = get_connections(connection, filter_type=connection_type)
        with open(path.join('catalog','connections_'+sysid+'.json'),'w') as fp:
            json.dump(connections,fp,indent=4)


    CONTAINER_CALL = False
    if CONTAINER_CALL :
        root = {'id': 'connectionRoot',
                     'name': 'Root',
                     'qualifiedName': '/',
                     'catalogObjectType': 'ROOT_FOLDER'}

        containers = dict()
        container_filter = 'connectionType eq \'SDL\''
        get_containers(connection, containers, root, container_filter=None)

        with open(path.join('catalogs','container_'+sysid+'.json'),'w') as fp:
            json.dump(containers,fp,indent=4)

    GET_DATASETS = False
    if GET_DATASETS :
        datasets = list()
        with open(path.join('catalogs','container_dh-1svpfuea.dhaas-live_default.json')) as fp:
            containers = json.load(fp)
        for name,container in containers.items() :
            dsets = get_datasets(connection, container['id'])
            if len(dsets) > 0:
                logging.info(f"Scanned container:{container['qualifiedName']} - #datasets:{len(dsets)}")
                datasets.extend(dsets)

        with open(path.join('catalogs','datasets_'+sysid+'.json'),'w') as fp:
            json.dump(datasets,fp,indent=4)

    GET_DATASET_SUMMARY = False
    if GET_DATASET_SUMMARY :
        with open(path.join('catalogs','datasets_dh-1svpfuea.dhaas-live_default.json'),'r') as fp:
            datasets = json.load(fp)
        datasets_cols = list()
        for dataset in datasets:
            ds = get_dataset_summary(connection, dataset['qualifiedName'], dataset['id'])
            if 'code' in ds and ds['code'] == '70016' :
                logging.warning(f"Read dataset summary for {dataset['qualifiedName']}: {ds['message']}")
                continue
            for c in ds['additionalInfo']['columns']:
                length = c['length'] if 'length' in c else ''
                datasets_cols.append({'connection_type':ds['remoteObject']['connectionType'],
                                     'connection_id':ds['remoteObject']['connectionId'],
                                     'dataset_path':ds['remoteObject']['qualifiedName'],
                                     'dataset_name':ds['remoteObject']['name'],
                                     'column_name': c['name'],
                                     'length': length,
                                     'dtype':c['templateType']})

        with open(path.join('catalogs','datasets_cols_dh-1svpfuea.dhaas-live_default.json'),'w') as fp:
            json.dump(datasets_cols, fp, indent=4)
        with open(path.join('catalogs','datasets_cols_dh-1svpfuea.dhaas-live_default.csv'),'w') as fp:
            csvwriter = csv.writer(fp)
            csvwriter.writerow(list(datasets_cols[0].keys()))
            for dsc in datasets_cols:
                csvwriter.writerow(list(dsc.values()))

    GET_ATTRIBUTES = False
    if GET_ATTRIBUTES :
        df = pd.read_csv(path.join('catalogs','datasets_cols_dh-1svpfuea.dhaas-live_default.csv'))
        df_ds = df.groupby(by=['connection_id','dataset_path'])['connection_type'].first().reset_index()

        for i,dsc in df_ds.iterrows() :
            dataset_attributes = get_dataset_tags(connection, dsc['dataset_path'], dsc['connection_id'])
            tags = dataset_attributes_str(dataset_attributes)
            df.loc[(df['connection_id'] == dsc['connection_id']) & (df['dataset_path'] == dsc['dataset_path']),'dataset_tags'] = tags['dataset_tags']
            for at,at_tags in tags['attribute_tags'].items() :
                df.loc[(df['connection_id'] == dsc['connection_id']) &
                       (df['dataset_path'] == dsc['dataset_path']) &
                       (df['column_name'] == at),'column_tags'] = at_tags

        df.to_csv(path.join('catalogs','datasets_cols_tags_'+sysid+'.csv'),index=False)

    ADD_ATTRIBUTES = False
    if ADD_ATTRIBUTES :
        with open(path.join('catalogs','dataset_tags.json'),'r') as fp:
            dataset_tags = json.load(fp)

        with open(path.join('catalogs','hierarchies.json'),'r') as fp:
            hierarchies = json.load(fp)

        for ds,dsv in dataset_tags.items():
            for dst in dsv['dataset_tags']:
                hierarchy_id = hierarchies[dst]['hierarchy_id']
                tag_id = hierarchies[dst]['tag_id']
                add_dataset_tag(connection, ds, hierarchy_id, tag_id)
            for attribute, ast in dsv['attribute_tags'].items():
                hierarchy_id = hierarchies[ast]['hierarchy_id']
                tag_id = hierarchies[ast]['tag_id']
                add_dataset_attribute_tag(connection, ds, hierarchy_id, tag_id, attribute)

    EXPORT = True
    if EXPORT :
        df = export_catalog(connection)
        df.to_csv(path.join('catalogs','metadata_'+sysid+'.csv'),index=False)

    AUTOTAG = False
    if AUTOTAG:
        metadata_df = pd.read_csv('catalogs/metadata_dh-1svpfuea.dhaas-live_default.csv')

        tagmap = list()
        with open('catalogs/auto_tagging.csv') as fp:
            csvreader = csv.reader(fp)
            header = next(csvreader)
            for row in csvreader:
                if row[0][0] == '#':
                    continue
                tagmap.append(row)

        auto_tag(connection,metadata_df,tagmap)




