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


def gen():
    host = "https://vsystem.ingress.xxxxxxxxx.dhaas-live.shoot.live.k8s-hana.ondemand.com"
    user = "xxxx"
    pwd = "xxxx"
    path = "/app/datahub-app-metadata/api/v1"
    tenant = 'default'

    connection = {'url': urljoin(host, path), 'auth': (tenant + '\\' + user, pwd)}

    # Config parameters
    connection_id = 'ECC_DMIS_2018'
    container_path = '/TABLES/BC'

    # Get all catalog datasets under container path
    datasets = get_datasets(connection, connection_id, container_path)

    dataset_factsheets = list()
    for i, ds in enumerate(datasets):
        qualified_name = ds['remoteObjectReference']['qualifiedName']
        dataset = get_dataset_factsheets(connection, connection_id, qualified_name)

        # In case of Error
        if dataset == -1:
            continue

        dataset['tags'] = get_dataset_tags(connection, connection_id, qualified_name)
        dataset_factsheets.append(dataset)

    datasets_json = json.dumps(dataset_factsheets, indent=4)
    print(datasets_json)


if __name__ == '__main__':
    gen()
