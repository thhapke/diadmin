# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import api

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
    api.logger.info(f"Request URL: {url}")
    r = requests.get(url, headers=headers, auth=connection['auth'])

    if r.status_code != 200:
        api.logger.error(f"Status code: {r.status_code}  - {r.text}")
        return None

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
        return None
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
        return None
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
        return None
    if r.status_code == 500:
        api.logger.error(f"Status code: {r.status_code}  - {r.text}")
        return None
    return json.loads(r.text)


#
# Callback of operator
#
def gen():
    host = api.config.http_connection['connectionProperties']['host']
    user = api.config.http_connection['connectionProperties']['user']
    pwd = api.config.http_connection['connectionProperties']['password']
    path = api.config.http_connection['connectionProperties']['path']
    tenant = os.environ.get('VSYSTEM_TENANT')

    if not tenant:
        tenant = 'default'

    connection = {'url': urljoin(host, path), 'auth': (tenant + '\\' + user, pwd)}

    # Config parameters
    connection_id = api.config.connection_id['connectionID']
    container_path = api.config.container
    tags = api.config.tags
    streaming = api.config.streaming
    lineage = api.config.lineage

    # Get all catalog datasets under container path
    api.logger.info(f"Get datasets: {connection_id} - {container_path}")
    datasets = get_datasets(connection, connection_id, container_path)
    
    if not datasets:
        api.logger.info(f"No Datasets for: {connection_id} - {container_path} -> shutdown pipeline")
        return None

    dataset_factsheets = list()
    for i, ds in enumerate(datasets):
        qualified_name = ds['remoteObjectReference']['qualifiedName']
        api.logger.info(f'Get dataset metadata: {qualified_name}')
        dataset = get_dataset_factsheets(connection, connection_id, qualified_name)

        # In case of Error (like imported data)
        if dataset == -1:
            continue

        if tags:
            dataset['tags'] = get_dataset_tags(connection, connection_id, qualified_name)

        if lineage:
            dataset['lineage'] = get_dataset_lineage(connection, connection_id, qualified_name)

        if streaming:
            if i == len(datasets)-1:
                header = [i, True, i + 1, len(datasets), ""]
            else:
                header = [i, False, i+1, len(datasets), ""]
            header = {"com.sap.headers.batch": header}
            datasets_json = json.dumps(dataset, indent=4)
            api.outputs.datasets.publish(datasets_json, header=header)
        else:
            dataset_factsheets.append(dataset)

    if not streaming:
        header = [0, True, 1, 0, ""]
        header = {"com.sap.headers.batch": header}
        datasets_json = json.dumps(dataset_factsheets, indent=4)
        api.outputs.datasets.publish(datasets_json, header=header)


api.set_prestart(gen)
