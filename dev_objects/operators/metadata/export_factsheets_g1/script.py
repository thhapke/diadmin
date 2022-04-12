# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import api


import os
from urllib.parse import urljoin
import urllib
import json

import requests

index = 0

# get the details of all datasets (not only published)
def get_dataset_factsheet(connection,connection_id,dataset_name):
    api.logger.info(f"Get Dataset factsheet: {dataset_name}")
    qualified_name = urllib.parse.quote(dataset_name,safe='')
    restapi = f'catalog/connections/{connection_id}/datasets/{qualified_name}/factsheets'
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'],verify=api.config.verify)
    if r.status_code != 200:
        api.logger.error("Get dataset factsheet: {}".format(r.text))
        api.send("logging",f"get_dataset_factsheet STATUS: {r.status_code} - {r.text}")
    return r.text

def get_lineage(connection,dataset_id) :
    restapi = f'catalog/datasets/{dataset_id}/lineage'
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'],verify=api.config.verify)

    response = json.loads(r.text)
    if r.status_code != 200:
        api.logger.error("Get lineage: {}".format(response['message']))
    return response

# crawling along path because the 'qualified Name' is not always matching the concatination of the container names
def get_container_qm(connection,connection_id,container_list,container_path,container) :
    if container == None:
        container = {'name': 'Root',
                     'qualifiedName': '/',
                     'parentQualifiedName':"/"}
        container_path = container_path.split('/')

    api.logger.info(f"Path trailing: {container['qualifiedName']}")
    container_list.append(container)

    if len(container_path) == 0 :
        return container_list
    next_folder = container_path.pop(0)
    while next_folder == '' and len(container_path) > 0 :
        next_folder = container_path.pop(0)
    if next_folder == '':
        return container_list

    qualified_name = urllib.parse.quote(container['qualifiedName'],safe='')
    api.logger.info(f"Get Connection Containers {connection_id} - {container['qualifiedName']}")
    restapi = f"catalog/connections/{connection_id}/containers/{qualified_name}/children"
    url = connection['url'][:-6] + restapi
    r = requests.get(url, headers={'X-Requested-With': 'XMLHttpRequest'}, auth=connection['auth'],verify=api.config.verify)

    if r.status_code != 200:
        api.logger.error(f"Status code: {r.status_code}, {r.text}")
        api.send("logging",f"Get_connection_datasets STATUS: {r.status_code} - {r.text}")
        return -1

    sub_containers = json.loads(r.text)['nodes']

    for c in sub_containers:
        if c['name'] == next_folder:
            if c['isContainer']  :
                get_container_qm(connection,connection_id,container_list,container_path,c)
            else :
                container_list.append(c)
                return container_list

def get_connection_datasets(connection,connection_id,container=None,lineage = False):
    global index
    if container == None :
        container = {'name': 'Root',
                     'qualifiedName': '/',
                     'parentQualifiedName':"/"}
    elif isinstance(container,str) :
        path_list = list()
        get_container_qm(connection,connection_id,path_list,container,None)
        container = path_list[-1]
        api.logger.info(f"Actual path: {container['qualifiedName']}")

    qualified_name = urllib.parse.quote(container['qualifiedName'],safe='')
    api.logger.info(f"Get Connection Containers {connection_id} - {container['qualifiedName']}")
    restapi = f"catalog/connections/{connection_id}/containers/{qualified_name}/children"
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'],verify=api.config.verify)

    if r.status_code != 200:
        api.logger.error(f"Status code: {r.status_code}, {r.text}")
        api.send("logging",f"Get_connection_datasets STATUS: {r.status_code} - {r.text}")
        return -1

    container_item = json.loads(r.text)
    if len(container_item['nodes']) > 0 :
        for c in container_item['nodes']:
            if c['isContainer'] :
                get_connection_datasets(connection,connection_id,c)
            else :
                ds = get_dataset_factsheet(connection,connection_id,c['qualifiedName'])
                if lineage :
                    fsheet = json.loads(ds)
                    for i,f in  enumerate(fsheet['factsheets']):
                        if f['metadata']['hasLineage']:
                            api.logger.info(f"Get Lineage of {f['metadata']['uri']}  (#{i})")
                            fsheet['lineage'] = get_lineage(connection,fsheet['datasetId'])
                    ds = json.dumps(fsheet,indent=4)
                att = {'index':index,'isLast':False,'count':10000000,'size':1000000,'unit':'unit','message.lastBatch':False}
                att['name'] = c['qualifiedName'].split('/',)[-1].split('.')[0]
                msg = api.Message(body=ds,attributes=att)
                api.send('factsheet',msg)
                index +=1
    else :
        ds = get_dataset_factsheet(connection,connection_id,container['qualifiedName'])
        if lineage :
            fsheet = json.loads(ds)
            for i,f in  enumerate(fsheet['factsheets']):
                if f['metadata']['hasLineage']:
                    api.logger.info(f"Get Lineage of {f['metadata']['uri']}  (#{i})")
                    fsheet['lineage'] = get_lineage(connection,fsheet['datasetId'])
            ds = json.dumps(fsheet,indent=4)


        att = {'index':index,'isLast':False,'count':10000000,'size':1000000,'unit':'unit','message.lastBatch':False}
        att['name'] = container['qualifiedName'].split('/',)[-1].split('.')[0]
        msg = api.Message(body=ds,attributes=att)
        api.send('factsheet',msg)
        index +=1


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
                                       container=api.config.path,lineage=api.config.lineage)

    att = {'index':index-1,'isLast':True,'count':index+1,'size':0,'unit':'unit','message.lastBatch':True}
    att['name'] = "EMPTY"
    msg = api.Message(body='',attributes=att)
    api.send("factsheet",msg)


api.add_generator(gen)