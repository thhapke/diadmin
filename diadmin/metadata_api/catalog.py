#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

import requests
import json
from os import path
import urllib
import logging
from os.path import join
import yaml
from pprint import pprint
from rdflib import Graph, term
import re


# read availibility_rdf
def read_availibility_rdf(url) :
    g = Graph()
    g.parse(url)

    # Get file namespace
    ns_file = ''
    for ns_prefix, namespace in g.namespaces():
        if '' == ns_prefix:
            ns_file = namespace
            break

    root = ns_file[ns_file.rfind('/')+1:]
    qresult = g.query("SELECT  ?subj ?obj WHERE { ?subj skos:prefLabel  ?obj}")

    tree = {'name':None,'description':None,'nodes':[]}
    for r in qresult :
        m = re.sub(ns_file,'', r['subj'] )
        if m == ns_file :
            continue
        elif not m:
            tree['name'] = root
            tree['description'] = r['obj'].n3(g.namespace_manager).strip('<>').split('@')[0].strip('"')
        else :
            nr = {'name':m[1:],'description':r['obj'].value,'nodes':[]}
            tree['nodes'].append(nr)
    return tree

######## GET #####################
#
# Get hierarchies without content/tags
#
def get_hierarchy_names(connection, search=None) :
    restapi = "/catalog/tagHierarchies"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {"$search":search} if search else {}
    logging.info(f"Get Hierarchies: {url}")
    r = requests.get(url, headers=headers, auth=connection['auth'], params = params)

    if r.status_code != 200:
        logging.error(format(r.text))
        return None

    response = json.loads(r.text)
    return response

# API Call
# get tags of specified hierarchy with content/tags
#
def get_hierarchy_tags(connection, hierarchy_id) :

    restapi = f"/catalog/tagHierarchies/{hierarchy_id}"
    url = connection['url'] + restapi
    params = {"hierarchyId":hierarchy_id,"withTags":True}
    headers = {'X-Requested-With': 'XMLHttpRequest'}

    r = requests.get(url, headers=headers, auth=connection['auth'],params=params)
    response = json.loads(r.text)

    if r.status_code != 200:
        logging.error(f"Get hierarchy: {response['message']}")

    return response

############# POST #############

#  API Call
#  Add hierarchy name
#
def add_hierarchy(connection, name, description) :
    logging.info(f"Add Hierarchy: {name}")

    restapi = "/catalog/tagHierarchies"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}

    data = {"name": name, "description": description}

    r = requests.post(url, headers=headers, auth=connection['auth'], data=data)

    response = json.loads(r.text)
    if r.status_code != 201:
        logging.error(f"Adding hierarchy: {response['message']}")

    return response


def add_tag(connection,hierarchy_id,parent_id,name,description,color='black') :
    logging.info(f"Add tag by id: {name}")

    # Tag with no parentID is set as root tag
    if parent_id :
        data = {"parentId": parent_id, "name": name, "description": description, "color": color}
    else :
        data = {"name": name, "description": description, "color": 'black'}

    restapi = f"/catalog/tagHierarchies/{hierarchy_id}/tags"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}

    r = requests.post(url,headers=headers, auth=connection['auth'],data=data)
    response = json.loads(r.text)
    if not r.status_code in [ 201, 400 ]  :
        logging.error(f"Adding tag status {r.status_code} - {response['message']}")
        return None

    return response


####################
#  Upload Hierarchies
####################
def upload_hierarchies(connection,new_hierarchies,hierarchies = None) :
    if not hierarchies :
        hierarchies = download_hierarchies(connection)
    # check for  new hierarchies
    new_hnames = set()
    for h in new_hierarchies.values() :
        new_hnames.add(h['hierarchy_name'])
    hnames = set()
    for h in hierarchies.values() :
        hnames.add(h['hierarchy_name'])
    new_hierarchy_names = new_hnames - hnames

    # add new hierarchies
    for nh in new_hierarchy_names :
        response = add_hierarchy(connection,new_hierarchies[nh]['name'],new_hierarchies[nh]['description'])
        new_hierarchy = {'hierarchy':response}
        exfrmt_hierarchy(hierarchies,new_hierarchy)

    new_tags = new_hierarchies.keys() - hierarchies.keys() - new_hierarchy_names
    # sort list to ensure that hierarchy tags are added ascending to path length
    new_tags = sorted(new_tags, key=len)
    for nt in new_tags :
        if new_hierarchies[nt]['parent_path']:
            parent_id = hierarchies[new_hierarchies[nt]['parent_path']]['tag_id']
        else :
            parent_id = None
        hierarchy_id = hierarchies[new_hierarchies[nt]['hierarchy_name']]['hierarchy_id']
        response = add_tag(connection,hierarchy_id,parent_id,new_hierarchies[nt]['name'],new_hierarchies[nt]['description'])
        if response :
            new_hierarchies[nt]['tag_id'] = response['id']
            new_hierarchies[nt]['hierarchy_id'] = response['hierarchyId']
            hierarchies[nt] = new_hierarchies[nt]

    return hierarchies

def download_hierarchies(connection,hierarchy_name=None) :
        hnames = get_hierarchy_names(connection, search=hierarchy_name)
        if not hnames :
            logging.error(f"No Hierarchies found")
            return 0
        hierarchies =dict()
        for h in hnames['tagHierarchies'] :
            hierarchy = get_hierarchy_tags(connection, h["tagHierarchy"]['id'])
            exfrmt_hierarchy(hierarchies,hierarchy)

        return hierarchies

def add_di_tag(hierarchies,hierarchy_name,hierarchy_id,parent_path,dinode):
    node = {'name':dinode['tagInfo']['tag']['name'],
            'description':dinode['tagInfo']['tag']['description'],
            'path':hierarchy_name+'/'+dinode['tagInfo']['tag']['path'].replace('.','/'),
            'tag_id':dinode['tagInfo']['tag']['id'],
            'parent_path': parent_path,
            'hierarchy_id':hierarchy_id,
            'hierarchy_name':hierarchy_name}
    hierarchies[node['path']] = node
    for n in dinode['children'] :
        add_di_tag(hierarchies,hierarchy_name,hierarchy_id,node['path'],n)

def exfrmt_hierarchy(hierarchies,di_hierarchy) :
    hierarchy_name = di_hierarchy['hierarchy']['hierarchyDescriptor']['name']
    logging.info(f'Convert to export format: {hierarchy_name}')
    hierarchy = {'name':hierarchy_name,
                 'description':di_hierarchy['hierarchy']['hierarchyDescriptor']['description'],
                 'path':di_hierarchy['hierarchy']['hierarchyDescriptor']['name'],
                 'tag_id': "",
                 'parent_path':"",
                 'hierarchy_id':di_hierarchy['hierarchy']['id'],
                 'hierarchy_name':hierarchy_name}
    hierarchies[hierarchy['name']] = hierarchy

    if 'content' in di_hierarchy :
        for c in di_hierarchy['content'] :
            add_di_tag(hierarchies,hierarchy['name'],hierarchy['hierarchy_id'],hierarchy['parent_path'],c)


#########
# MAIN
########

def main() :
    logging.basicConfig(level=logging.INFO)

    with open('config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    conn = {'url':params['URL']+'/app/datahub-app-metadata/api/v1',
            'auth':(params['TENANT']+'\\'+ params['USER'],params['PWD'])}

    UPLOAD = True
    hierarchy_filename = 'License.json'
    if UPLOAD :
        with open(path.join('catalogs',hierarchy_filename),'r') as fp:
            new_hierarchies = json.load(fp)

        with open(path.join('catalogs','hierarchies.json'),'r') as fp:
            hierarchies = json.load(fp)

        hierarchies = None
        hierarchies = upload_hierarchies(conn,new_hierarchies,hierarchies)

    #hierarchy_name = 'License'
    #hierarchy_name = None
    DOWNLOAD_HIERARCHIES = False
    if DOWNLOAD_HIERARCHIES :
        hierarchies = download_hierarchies(conn,hierarchy_name=hierarchy_name)
        filename = 'hierarchies.json'
        if hierarchy_name :
            filename = hierarchy_name + '.json'
        with open(path.join('catalogs',filename),'w') as fp:
            json.dump(hierarchies,fp,indent=4)



if __name__ == '__main__' :
    main()