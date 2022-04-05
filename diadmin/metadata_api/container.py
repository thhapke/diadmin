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
import csv


import requests
import json
import yaml
import pandas as pd


from diadmin.utils.utils import get_system_id
from diadmin.metadata_api.catalog import download_hierarchies, add_catalog_neo4j
from diadmin.metadata_api.connection import get_connections, add_connections_graphdb, get_connection_details
from diadmin.connect.connect_neo4j import neo4jConnection


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



### CONTAINERS
def get_containers(connection,containers,container = None,container_filter=None):

    if container == None :
        container = {'id': 'connectionRoot',
                     'name': 'Root',
                     'qualifiedName': '/',
                     'catalogObjectType': 'ROOT_FOLDER'}

    # exclude internal DI_Data_Lake
    #if re.match('\/*DI_DATA_LAKE',container['qualifiedName']) :
    #    return 1

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
        logging.error(f"Status code: {r.status_code}  - {r.text}")
        return -1

    sub_containers = json.loads(r.text)['containers']
    for c in sub_containers:
        get_containers(connection,containers,c,container_filter=container_filter)
    return

def get_datasets(connection, connection_id, container_name) :
    logging.info(f"Get container of {container_name}")
    container_name = urllib.parse.quote(container_name,safe='')
    restapi = f"/catalog/connections/{connection_id}/containers/{container_name}"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'])

    if r.status_code != 200:
        logging.error(f"Status code: {r.status_code}  - {r.text}")
        return -1

    return json.loads(r.text)['datasets']

def get_ids(connection, datasets_ids, containers_ids, container=None) :
    if container == None:
        container = {'id': 'connectionRoot',
                     'name': 'Root',
                     'qualifiedName': '/',
                     'catalogObjectType': 'ROOT_FOLDER'}
    if not container['id'] == 'connectionRoot' :
        datasets_ids[container['id']] = container['qualifiedName']

    restapi = f"/catalog/containers/{container['id']}/children"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'])

    if r.status_code != 200:
        logging.error(f"Status code: {r.status_code}  - {r.text}")
        return -1

    sub_containers = json.loads(r.text)['containers']
    for c in sub_containers:
        get_ids(connection, datasets_ids, containers_ids,c)
    return

def get_connection_datasets(connection,connection_id,datasets=None,container=None,with_details = False):
    if datasets == None:
        datasets = dict()
    if container == None :
        container = {'name': 'Root',
                     'qualifiedName': '/',
                     'parentQualifiedName':"/"}
    elif isinstance(container,str) :
        path_elements = container.split('/')
        container = {'name': path_elements[-1],
                     'qualifiedName': container,
                     'parentQualifiedName':'/'.join(path_elements[:-1])}

    qualified_name = urllib.parse.quote(container['qualifiedName'],safe='')
    logging.info(f"Get Connection Containers {connection_id} - {container['qualifiedName']}")
    restapi = f"catalog/connections/{connection_id}/containers/{qualified_name}/children"
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'])

    if r.status_code != 200:
        logging.error(f"Status code: {r.status_code}, {r.text}")
        return -1

    sub_containers = json.loads(r.text)['nodes']
    for c in sub_containers:
        if c['isContainer'] :
            get_connection_datasets(connection,connection_id,datasets,c)
        else :
            if not with_details :
                datasets[c['qualifiedName']] = c
            else :
                ds = get_dataset_factsheet(connection,connection_id,c['qualifiedName'])
                datasets[c['qualifiedName']] = ds

    return datasets

def compare_snapshots(latest,current) :
    new_datasets = [ current[connection][dataset] for connection in current for dataset in current[connection]
                     if not dataset in latest[connection].keys()]
    return new_datasets


# add GRAPHDB
def add_containers_graphdb(gdb,containers) :
    for c in containers.values() :
        node_container = {'label':'CONTAINER',
                          'properties':{'id':c['id'],'name':c['name'],'qualifiedName':c['qualifiedName']},
                          'keys':['id']}
        gdb.create_node(node_container)
        if c['parentId'] == 'connectionRoot' :
            node_connection =  {'label':'CONNECTION',
                                'properties':{'id':c['connectionId']}}
            relationship_to = {'node_from':node_container,'node_to':node_connection,'relation':{'label':'PARENT'}}
            relationship_from = {'node_from':node_connection,'node_to':node_container,'relation':{'label':'CONTAINER'}}
        else :
            node_parent =  {'label':'CONTAINER',
                            'properties':{'id':c['parentId']}}
            relationship_to = {'node_from':node_container,'node_to':node_parent,'relation':{'label':'PARENT_CONTAINER'}}
            relationship_from = {'node_from':node_parent,'node_to':node_container,'relation':{'label':'CHILD_CONTAINER'}}
        gdb.create_relationship(relationship_to)
        gdb.create_relationship(relationship_from)

### DATASETS
def get_datasets_container(connection, container_id):
    restapi = f"/catalog/containers/{container_id}/children"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {"containerId": container_id, "type": "DATASET"}
    r = requests.get(url, headers=headers, auth=connection['auth'], params=params)

    response = json.loads(r.text)
    if r.status_code != 200:
        logging.error("Get metadata_datasets: {}".format(response['message']))

    return response['datasets']

# add GRAPHDB
def add_dataset_graphdb(gdb,dataset) :
    node_container = {'label':'CONTAINER','properties':{'id':dataset['parentId']}}
    node_dataset = { 'label':'DATASET',
                     'properties':{'id':dataset['id'],'name':dataset['name'],'path':dataset['qualifiedName'],
                                   'connection_id':dataset['connectionId']}}
    gdb.create_node(node_dataset)
    relationship_from = {'node_from':node_container,'node_to':node_dataset,'relation':{'label':'DATASET'}}
    gdb.create_relationship(relationship_from)
    relationship_to = {'node_from':node_dataset,'node_to':node_container,'relation':{'label':'CONTAINER'}}
    gdb.create_relationship(relationship_to)

### DATASET ATTRIBUTES
def get_dataset_summary(connection,dataset_name,dataset_id):
    logging.info(f"Get Dataset summary: {dataset_name}")
    restapi = f'catalog/datasets/{dataset_id}/summary'
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'])

    response = json.loads(r.text)
    if r.status_code != 200:
        logging.error("Get metadata_datasets: {}".format(response['message']))
    return response

### DATASET ATTRIBUTES
def get_dataset_factsheet(connection,connection_id,dataset_name):
    logging.info(f"Get Dataset factsheet: {dataset_name}")
    qualified_name = urllib.parse.quote(dataset_name,safe='')
    restapi = f'catalog/connections/{connection_id}/datasets/{qualified_name}/factsheets'
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'])

    response = json.loads(r.text)
    if r.status_code != 200:
        logging.error("Get dataset factsheet: {}".format(response['message']))
    return response

# add GRAPHDB
def add_dataset_attribute_graphdb(gdb,dataset_id,attribute) :
    node_dataset = { 'label':'DATASET','properties':{'id':dataset_id}}
    node_attribute  = {'label':'ATTRIBUTE',
                       'properties':{'id':attribute['name'],'dataset_id':dataset_id,'datatype':attribute['datatype']},
                       'keys':['id','dataset_id']}
    if 'length' in attribute:
        node_attribute['properties']['length'] = attribute['length']

    gdb.create_node(node_attribute)
    relation_to = {'node_from':node_dataset,'node_to':node_attribute,'relation':{'label':'ATTRIBUTE'}}
    gdb.create_relationship(relation_to)
    relation_from = {'node_from':node_attribute,'node_to':node_dataset,'relation':{'label':'DATASET'}}
    gdb.create_relationship(relation_from)

#### DATASET TAGS
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
        logging.error(f"Get datasets attributes unsuccessful for {dataset_path}: {r.status_code}\n{json.loads(r.text)}")
        return None
    return json.loads(r.text)

# add GRAPHDB
def add_tag_relationship_graphdb(gdb,dataset_id,dataset_tags) :
    dnode = {'label':'DATASET','properties':{'id':dataset_id}}
    for dt in dataset_tags['tagsOnDataset']:
        for t in dt['tags'] :
            tnode = {'label':'CATALOG_TAG','properties':{'id':t['tag']['id']}}
            relation_to = {'node_from':dnode,'node_to':tnode,'relation':{'label':'HAS_TAG'}}
            relation_from = {'node_from':tnode,'node_to':dnode,'relation':{'label':'TAG'}}
            gdb.create_relationship(relation_from)
            gdb.create_relationship(relation_to)

def export_catalog(connection) :
    containers = dict()
    datasets = list()
    datasets_cols = list()
    containers = get_containers(connection,containers)
    for name,container in containers.items() :
        dsets = get_datasets_container(connection, container['id'])
        if dsets and len(dsets) > 0:
            logging.info(f"Scanned container:{container['qualifiedName']} - #datasets:{len(dsets)}")
            datasets.extend(dsets)

    for dataset in datasets:
        ds = get_dataset_summary(connection,dataset['qualifiedName'],dataset['id'])
        if 'code' in ds and ds['code'] == '70016' :
            logging.warning(f"Read dataset summary for {dataset['qualifiedName']}: {ds['message']}")
            continue
        if 'code' in ds and ds['code'] == 'internal' :
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
        if not dataset_attributes :
            logging.warning("No dataset attributes!")
            continue
        tags = dataset_attributes_str(dataset_attributes)
        df.loc[(df['connection_id'] == dsc['connection_id']) & (df['dataset_path'] == dsc['dataset_path']),'dataset_tags'] = tags['dataset_tags']
        for at,at_tags in tags['attribute_tags'].items() :
            df.loc[(df['connection_id'] == dsc['connection_id']) &
                   (df['dataset_path'] == dsc['dataset_path']) &
                   (df['column_name'] == at),'column_tags'] = at_tags

    return df


def export_graphdb(connection):
    gdb = neo4jConnection( connection['GRAPHDB']['URL']+':'+str(connection['GRAPHDB']['PORT']),
                           connection['GRAPHDB']['USER'], connection['GRAPHDB']['PWD'],connection['GRAPHDB']['DB'])

    # CATALOG
    add_catalog_neo4j(connection)

    # TENANT
    node_tenant = {'label':'TENANT',
                   'properties':{'tenant':connection['TENANT'],'url':connection['URL']}}
    gdb.create_node(node_tenant)

    # CONNECTIONS
    connections = get_connections(connection)
    add_connections_graphdb(gdb,connection,connections)

    # CONTAINER
    containers = dict()
    get_containers(connection, containers)
    add_containers_graphdb(gdb,containers)

    # DATASETS
    for name,container in containers.items() :
        datasets = get_datasets_container(connection, container['id'])
        for dataset in datasets :
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


# Add tag to dataset
def add_dataset_tag(connection,dataset_path,hierarchy_id,tag_id,connection_id=None) :

    path_list = dataset_path.strip('/').split("/")
    if connection_id == None or connection_id == path_list[0]:
        connection_id = path_list[0]
        qualified_name = '/'+'/'.join(path_list[1:])
    else :
        qualified_name = '/'+'/'.join(path_list[0:])

    qualified_name = urllib.parse.quote(qualified_name,safe='')
    restapi = f"/catalog/connections/{connection_id}/metadata_datasets/{qualified_name}/tagHierarchies/{hierarchy_id}/tags"
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
    restapi = f"/catalog/connections/{connection_id}/metadata_datasets/{qualified_name}/attributes/{attributes_qn}/tagHierarchies/{hierarchy_id}/tags"
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


# Trigger Profiling
# WARNING: Private API - unsupported
def start_profiling(connection,connection_id,connection_type,qualified_name) :
    logging.info(f"Start Profiling: {qualified_name}")
    restapi = f'catalog/connections/{connection_id}/datasets/{qualified_name}/factsheets'
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {'connectionTypye':connection_type}
    r = requests.post(url, headers=headers, auth=connection['auth'],params=params)

    response = json.loads(r.text)
    if r.status_code != 202:
        logging.error("Start Profiling: {}".format(response['message']))
    return response


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

    NEO4J = False
    if NEO4J:
        export_graphdb(connection)

    GET_DATASETS = False
    if GET_DATASETS :
        connection_id = 'HANA_Cloud_DQM'
        datasets = get_datasets(connection, connection_id=connection_id, container_name='/')
        for dataset in datasets:
            q_name = dataset['remoteObjectReference']['qualifiedName']
            dataset['tags'] = get_dataset_tags(connection,q_name,connection_id=connection_id,tag_hierarchies=None)
        print(json.dumps(datasets,indent=4))

    CONNECTIONS = False
    if CONNECTIONS :
        #connection_type = "SDL"
        connection_type = ''
        catalog_connections = False
        connections = get_connections(connection, filter_type=connection_type)

        with open(path.join('catalogs','catalog_connections_'+sysid+'.json'),'w') as fp:
            json.dump(connections,fp,indent=4)

    ALL_CONNECTION_DATASETS = False
    if ALL_CONNECTION_DATASETS:
            connections = get_connections(connection)
            with open(path.join('catalogs','all_connections_'+sysid+'.json'),'w') as fp:
                json.dump(connections,fp,indent=4)
            datasets = dict()
            #for connection_id in ['ECC_IDES_GCP'] :
            #    datasets[connection_id] = get_connection_datasets(connection,connection_id)
            container = '/ODP_SAPI/SAP'
            container = {'name':'SAP-R/3','qualifiedName':'/ODP_SAPI/SAP/SAP-R/3','parentQualifiedName':'/ODP_SAPI/SAP'}
            datasets = get_connection_datasets(connection,connection_id=g.add((nsys[t['path']],ndi.parentTag,nsys[t['parent_path']])),container=container,with_details=True)

            #with open(path.join('connections/snapshots','connection_datasets_'+sysid+'.json')) as fp:
            #    latest_snapshot = json.load(fp)
            #datasets = compare_snapshots(latest_snapshot,datasets)

            #connection_id = 'S3_Catalog'
            #connection_details = get_connection_details(connection,connection_id)
            #connection_type = connection_details['type']
            # Start profiling
            #for dataset in new_datasets:
            #    start_profiling(connection,connection_id,connection_type,dataset['qualifiedName'])

            if len(datasets) >0 :
                with open(path.join('connections/snapshots','connection_datasets_'+sysid+'.json'),'w') as fp:
                    json.dump(datasets,fp,indent=4)

    DATASET_ID = True
    if DATASET_ID :
        connection_id = 'HANA_Cloud_DQM'
        dataset_qm = '/HANA_Cloud_DQM/QMGMT/CSKA'
        datasets_ids = dict()
        containers_ids = dict()
        get_ids(connection, datasets_ids,containers_ids)
        print(json.dumps(datasets_ids,indent=4))
        print(json.dumps(containers_ids,indent=4))

    CONTAINER_CALL = False
    if CONTAINER_CALL :
        root = {'id': 'connectionRoot',
                     'name': 'Root',
                     'qualifiedName': '/',
                     'catalogObjectType': 'ROOT_FOLDER'}

        containers = dict()
        filter = 'connectionType eq \'ABAP\''
        filter = "name eq 'HANA_Cloud_DQM'"
        get_containers(connection, containers, root, container_filter=filter)
        print(json.dumps(containers,indent=4))

        #with open(path.join('catalogs','container_'+sysid+'.json'),'w') as fp:
        #    json.dump(containers,fp,indent=4)

    GET_DATASETS2 = False
    if GET_DATASETS2 :
        datasets = list()
        container_id ='E37E03883CE6CAE74EDB0F1D06FDBB87F919638764B727594ECBC6794D5AD685D8DF11F38A46083FAA5D04A078120AD2DC9B77A7FD00084D8F8DF3B4EE60256D'

        dsets = get_datasets_container(connection, container_id=None,container_filter=filter)
        print(json.dumps(dsets,indent=4))
        #with open(path.join('catalogs','container_dh-1svpfuea.dhaas-live_default.json')) as fp:
        #    containers = json.load(fp)
        #for name,container in containers.items() :
        #    dsets = get_datasets_container(connection, container['id'])

        #    if len(dsets) > 0:
        #        logging.info(f"Scanned container:{container['qualifiedName']} - #metadata_datasets:{len(dsets)}")
        #        datasets.extend(dsets)

        #with open(path.join('catalogs','datasets_'+sysid+'.json'),'w') as fp:
        #    json.dump(datasets,fp,indent=4)

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
                template = c['templateType'] if 'templateType' in c else ''
                datasets_cols.append({'connection_type':ds['remoteObject']['connectionType'],
                                     'connection_id':ds['remoteObject']['connectionId'],
                                     'dataset_path':ds['remoteObject']['qualifiedName'],
                                     'dataset_name':ds['remoteObject']['name'],
                                     'column_name': c['name'],
                                     'length': length,
                                     'dtype':template})

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

    EXPORT = False
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




