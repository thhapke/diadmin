# Mock apis needs to be commented before used within SAP Data Intelligence
from diadmin.dimockapi.mock_api import api

import os
from urllib.parse import urljoin
import json

import requests


#
# GET tags of specified hierarchy with content/tags
#
def get_hierarchy_tags(connection, hierarchy_id):
    restapi = f"/catalog/tagHierarchies/{hierarchy_id}"
    url = connection['url'] + restapi
    params = {"hierarchyId": hierarchy_id, "withTags": True}
    headers = {'X-Requested-With': 'XMLHttpRequest'}

    r = requests.get(url, headers=headers, auth=connection['auth'], params=params)
    response = json.loads(r.text)

    if r.status_code != 200:
        api.logger.error(f"Get hierarchy: {response['message']}")

    return response


#
# Get hierarchies without content/tags
#
def get_hierarchy_names(connection, search=None):
    restapi = "/catalog/tagHierarchies"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {"$search": search} if search else {}
    api.logger.info(f"Get Hierarchies: {url}")
    r = requests.get(url, headers=headers, auth=connection['auth'], params=params)

    if r.status_code != 200:
        api.logger.error(format(r.text))
        return None

    response = json.loads(r.text)
    return response


#
# Add tags to formatted output
#
def add_di_tag(hierarchies, hierarchy_name, hierarchy_id, parent_path, dinode):
    node = {'name': dinode['tagInfo']['tag']['name'],
            'description': dinode['tagInfo']['tag']['description'],
            'path': hierarchy_name + '/' + dinode['tagInfo']['tag']['path'].replace('.', '/'),
            'tag_id': dinode['tagInfo']['tag']['id'],
            'parent_path': parent_path,
            'hierarchy_id': hierarchy_id,
            'hierarchy_name': hierarchy_name}
    hierarchies[node['path']] = node
    for n in dinode['children']:
        add_di_tag(hierarchies, hierarchy_name, hierarchy_id, node['path'], n)


#
# Create formatted output
#
def exfrmt_hierarchy(hierarchies, di_hierarchy):
    hierarchy_name: object = di_hierarchy['hierarchy']['hierarchyDescriptor']['name']
    api.logger.info(f'Convert to export format: {hierarchy_name}')
    hierarchy = {'name': hierarchy_name,
                 'description': di_hierarchy['hierarchy']['hierarchyDescriptor']['description'],
                 'path': di_hierarchy['hierarchy']['hierarchyDescriptor']['name'],
                 'tag_id': "",
                 'parent_path': "",
                 'hierarchy_id': di_hierarchy['hierarchy']['id'],
                 'hierarchy_name': hierarchy_name}
    hierarchies[hierarchy['name']] = hierarchy

    if 'content' in di_hierarchy:
        for c in di_hierarchy['content']:
            add_di_tag(hierarchies, hierarchy['name'], hierarchy['hierarchy_id'], hierarchy['parent_path'], c)


#
# Get hierarchies with content/tags
#
def export_hierarchies(connection, hierarchy_name=None):
    hierarchy_names = get_hierarchy_names(connection, search=hierarchy_name)
    if not hierarchy_names:
        api.logger.error(f"No Hierarchies found")
        return 0
    hierarchies = dict()
    for h in hierarchy_names['tagHierarchies']:
        hierarchy: object = get_hierarchy_tags(connection, h["tagHierarchy"]['id'])
        exfrmt_hierarchy(hierarchies, hierarchy)

    return hierarchies


#
# CALLBACK function of operator
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

    hierarchies = export_hierarchies(connection, hierarchy_name=api.config.hierarchy_name)

    header = [0, True, 1, 0, ""]
    header = {"com.sap.headers.batch": header}
    hierarchies_json = json.dumps(hierarchies, indent=4)
    api.outputs.output.publish(hierarchies_json, header=header)


api.set_prestart(gen)
