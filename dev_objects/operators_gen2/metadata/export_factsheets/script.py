# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import api

import os
from urllib.parse import urljoin
import urllib
import json

import requests

index = 0

def get_dataset_factsheet(connection,connection_id,dataset_name):
    api.logger.info(f"Get Dataset factsheet: {dataset_name}")
    qualified_name = urllib.parse.quote(dataset_name,safe='')
    restapi = f'catalog/connections/{connection_id}/datasets/{qualified_name}/factsheets'
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'])
    if r.status_code != 200:
        api.logger.error("Get dataset factsheet: {}".format(r.text))
        api.outputs.logging.publish(body=f"get_dataset_factsheet STATUS: {r.status_code} - {r.text}",header=None)
    return r.text

def get_connection_datasets(connection,connection_id,container=None,with_details = False):
    global index
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
        api.outputs.logging.publish(body=f"Get_connection_datasets STATUS: {r.status_code} - {r.text}",header=None)
        return -1

    sub_containers = json.loads(r.text)['nodes']
    for c in sub_containers:
        if c['isContainer'] :
            get_connection_datasets(connection,connection_id,c)
        else :
            header = api.Record([index,True,0,0,""])
            header = {"com.sap.headers.batch":header}
            index +=1
            if not with_details :
                api.outputs.factsheet.publish(c,header=header)
            else :
                ds = get_dataset_factsheet(connection,connection_id,c['qualifiedName'])
                api.outputs.factsheet.publish(ds,header=header)


def gen():

    global index
    host = api.config.http_connection['connectionProperties']['host']
    user = api.config.http_connection['connectionProperties']['user']
    pwd = api.config.http_connection['connectionProperties']['password']
    path = api.config.http_connection['connectionProperties']['path']
    tenant = os.environ.get('VSYSTEM_TENANT')

    if not tenant:
        tenant = 'default'

    conn = {'url':urljoin(host,path),'auth':(tenant+'\\'+ user,pwd)}

    datasets = get_connection_datasets(conn,connection_id=api.config.connection_id,
                                       container=api.config.container,with_details=True)


    header = api.Record([index-1,True,index,0,""])
    header = {"com.sap.headers.batch":header}
    api.outputs.factsheet.publish("",header=header)


api.set_prestart(gen)