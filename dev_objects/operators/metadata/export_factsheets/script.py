# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import api

import os
from urllib.parse import urljoin
import urllib
import json

import requests

def get_dataset_factsheet(connection,connection_id,dataset_name):
    api.logger.info(f"Get Dataset factsheet: {dataset_name}")
    qualified_name = urllib.parse.quote(dataset_name,safe='')
    restapi = f'catalog/connections/{connection_id}/datasets/{qualified_name}/factsheets'
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'])
    if r.status_code != 200:
        api.logger.error("Get dataset factsheet: {}".format(r.text))
        api.send('logging',f"get_dataset_factsheet STATUS: {r.status_code} - {r.text}") 
    return r.text

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
    api.logger.info(f"Get Connection Containers {connection_id} - {container['qualifiedName']}")
    restapi = f"catalog/connections/{connection_id}/containers/{qualified_name}/children"
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'])

    if r.status_code != 200:
        api.logger.error(f"Status code: {r.status_code}, {r.text}")
        api.send('logging',f"Get_connection_datasets STATUS: {r.status_code} - {r.text}") 
        return -1

    sub_containers = json.loads(r.text)['nodes']
    for c in sub_containers:
        if c['isContainer'] :
            get_connection_datasets(connection,connection_id,datasets,c)
        else :
            if not with_details :
                #datasets[c['qualifiedName']] = c
                att = {'dataset':c['qualifiedName'],'with_details':with_details,'message.lastBatch':False}
                msg_output = api.Message(attributes=att,body=c)
                api.send('output',msg_output)  # dev_data type: message
            else :
                ds = get_dataset_factsheet(connection,connection_id,c['qualifiedName'])
                datasets[c['qualifiedName']] = ds
                att = {'dataset':c['qualifiedName'],'with_details':with_details,'message.lastBatch':False}
                msg_output = api.Message(attributes=att,body=ds)
                api.send('output',msg_output)  # dev_data type: message

    return datasets


def gen():

    host = api.config.http_connection['connectionProperties']['host']
    user = api.config.http_connection['connectionProperties']['user']
    pwd = api.config.http_connection['connectionProperties']['password']
    path = api.config.http_connection['connectionProperties']['path']
    tenant = os.environ.get('VSYSTEM_TENANT') if os.environ.get('VSYSTEM_TENANT') else 'default'

    conn = {'url':urljoin(host,path),'auth':(tenant+'\\'+ user,pwd)}
    
    api.send('logging',f"User: {user}  PWD: {pwd}  TENANT: {tenant}  HOST: {conn['url']}")

    container = api.config.container
    if api.config.container[0] == '{' :
        container = json.loads(api.config.container)

    datasets = get_connection_datasets(conn,connection_id=api.config.connection_id,
                                       container=container,with_details=True)
                                       
    msg_output = api.Message(attributes={'message.lastBatch':True},body="")
    api.send('output',msg_output)  # dev_data type: message


api.add_generator(gen)