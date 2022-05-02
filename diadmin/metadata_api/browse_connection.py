
from urllib.parse import urljoin
import urllib
import json
import logging

import requests

index = 0
VERIFICATION = True


# get the details of all datasets (not only published)
def get_dataset_factsheet(connection, connection_id, dataset_name):
    logging.info(f"Get Dataset factsheet: {dataset_name}")
    qualified_name = urllib.parse.quote(dataset_name, safe='')
    restapi = f'catalog/connections/{connection_id}/datasets/{qualified_name}/factsheets'
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'], verify=VERIFICATION)
    if r.status_code != 200:
        logging.error("Get dataset factsheet: {}".format(r.text))
        return None
    return r.text


def get_lineage(connection,dataset_id):
    restapi = f'catalog/datasets/{dataset_id}/lineage'
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'], verify=VERIFICATION)

    response = json.loads(r.text)
    if r.status_code != 200:
        logging.error("Get lineage: {}".format(response['message']))
    return response


# crawling along path because the 'qualified Name' is not always matching the concatination of the container names
def get_container_qm(connection, connection_id, container_list, container_path, container):
    if not container:
        container = {'name': 'Root',
                     'qualifiedName': '/',
                     'parentQualifiedName': "/"}
        container_path = container_path.split('/')

    logging.info(f"Path trailing: {container['qualifiedName']}")
    container_list.append(container)

    if len(container_path) == 0:
        return container_list
    next_folder = container_path.pop(0)
    while next_folder == '' and len(container_path) > 0:
        next_folder = container_path.pop(0)
    if next_folder == '':
        return container_list

    qualified_name = urllib.parse.quote(container['qualifiedName'], safe='')
    logging.info(f"Get Connection Containers {connection_id} - {container['qualifiedName']}")
    restapi = f"catalog/connections/{connection_id}/containers/{qualified_name}/children"
    url = connection['url'][:-6] + restapi
    r = requests.get(url, headers={'X-Requested-With': 'XMLHttpRequest'}, auth=connection['auth'], verify=VERIFICATION)

    if r.status_code != 200:
        logging.error(f"Status code: {r.status_code}, {r.text}")
        return -1

    sub_containers = json.loads(r.text)['nodes']

    #next_folder = next_folder.upper()
    for c in sub_containers:
        #c['qualifiedName'] = c['name'].upper()
        if c['qualifiedName'] == next_folder or c['qualifiedName'] == '/'+next_folder:
            if c['isContainer']:
                get_container_qm(connection, connection_id, container_list, container_path, c)
            else:
                container_list.append(c)
                return container_list


def get_connection_datasets(connection, connection_id,factsheets, container=None, lineage=False):
    global index
    if not container:
        qualified_name = '/'
    elif isinstance(container, str):
        qualified_name = container
    else:
        qualified_name = container['qualifiedName']

    '''
    if not container:
        container = {'name': 'Root',
                     'qualifiedName': '/',
                     'parentQualifiedName': "/"}
    elif isinstance(container, str):
        path_list = list()
        get_container_qm(connection, connection_id, path_list, container, None)
        container = path_list[-1]
        logging.info(f"Actual path: {container['qualifiedName']}")
    '''

    #qualified_name = urllib.parse.quote(container['qualifiedName'], safe='')
    qualified_name_quote = urllib.parse.quote(qualified_name, safe='')

    logging.info(f"Get Connection Containers {connection_id} - {qualified_name}")
    restapi = f"catalog/connections/{connection_id}/containers/{qualified_name_quote}/children"
    url = connection['url'][:-6] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'], verify=VERIFICATION)

    if r.status_code != 200:
        logging.error(f"Status code: {r.status_code}, {r.text}")
        return -1

    container_item = json.loads(r.text)
    if len(container_item['nodes']) > 0:
        for item in container_item['nodes']:
            if item['qualifiedName'] == qualified_name:
                if item['isContainer']:
                    get_connection_datasets(connection, connection_id, item)
                else:
                    ds = get_dataset_factsheet(connection, connection_id, item['qualifiedName'])
                    fact_sheet = json.loads(ds)
                    if lineage:
                        for i, f in enumerate(fact_sheet['factsheets']):
                            if f['metadata']['hasLineage']:
                                logging.info(f"Get Lineage of {f['metadata']['uri']}  (#{i})")
                                fact_sheet['lineage'] = get_lineage(connection, fact_sheet['datasetId'])
                        ds = json.dumps(fact_sheet, indent=4)
                    att = {'index': index, 'isLast': False, 'count': 10000000, 'size': 1000000, 'unit': 'unit',
                           'message.lastBatch': False, 'name': item['qualifiedName'].split('/', )[-1].split('.')[0]}
                    # msg = api.Message(body=ds,attributes=att)
                    # api.send('factsheet',msg)
                    factsheets.append(fact_sheet)
                    index += 1
    else:
        ds = get_dataset_factsheet(connection, connection_id, container['qualifiedName'])
        fact_sheet = json.loads(ds)
        if lineage:
            for i, f in enumerate(fact_sheet['factsheets']):
                if f['metadata']['hasLineage']:
                    logging.info(f"Get Lineage of {f['metadata']['uri']}  (#{i})")
                    fact_sheet['lineage'] = get_lineage(connection, fact_sheet['datasetId'])
            ds = json.dumps(fact_sheet, indent=4)

        att = {'index': index, 'isLast': False, 'count': 10000000, 'size': 1000000, 'unit': 'unit',
               'message.lastBatch': False, 'name': container['qualifiedName'].split('/', )[-1].split('.')[0]}
        # msg = api.Message(body=ds,attributes=att)
        # api.send('factsheet',msg)
        factsheets.append(fact_sheet)
        index += 1