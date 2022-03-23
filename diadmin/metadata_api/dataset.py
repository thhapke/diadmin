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
import json
from os import path
from urllib.parse import urljoin

import yaml
import requests


def add_dataset(connection,dataset) :

    url = connection['url'] + '/catalog/metadata_datasets'
    logging.info(f'Add dataset metadata: {url}')
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.post(url, headers=headers, auth=connection['auth'], json = dataset)

    response = json.loads(r.text)
    if r.status_code != 202:
        logging.error(f"Adding dataset metadata: {response['message']}  status: {r.status_code}\n{response}")
        return None

    return response

def check_status_task(connection,task_id) :
    url = connection['url'] + f'/catalog/importTasks/{task_id}'
    logging.info(f'Status of import: {url}')
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers, auth=connection['auth'])

    response = json.loads(r.text)
    if r.status_code != 200:
        logging.error(f"Check TaskID status: {response['message']}  status: {r.status_code}\n{response}")

    return response



def main() :
    logging.basicConfig(level=logging.INFO)

    with open('config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    conn = {'url':params['URL']+'/app/datahub-app-metadata/api/v1',
            'auth':(params['TENANT']+'\\'+ params['USER'],params['PWD'])}

    with open(path.join("metadata_datasets","minimal_dataset.json")) as fp:
        dataset_meta = json.load(fp)

    r = add_dataset(conn,dataset_meta)
    if r :
        r = check_status_task(conn,r['importTaskId'])

    print(r)

if __name__ == '__main__' :
    main()
