#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
########################################################
#### Using private RestAPI for connecting to Glossaries
########################################################


import logging
import json
import requests
import yaml



def get_glossaries(connection) :
    restapi = f"/glossaries"
    url = connection['url'] + restapi
    #params = {"hierarchyId":hierarchy_id,"withTags":True}
    headers = {'X-Requested-With': 'XMLHttpRequest'}

    r = requests.get(url, headers=headers, auth=connection['auth'])
    response = json.loads(r.text)

    if r.status_code != 200:
        logging.error(f"Get glossaries: {response['message']}")

    return response

def get_glossary_id(connection,glossary_name) :

    glossaries = get_glossaries(connection)

    for g in glossaries["glossaries"] :
        if glossary_name == g["name"] :
            return g["id"]

    glossary_names = [ g["name"] for g in glossaries["glossaries"] ]

    logging.error(f"Glossary \"{glossary_name}\" not found in glossary list: {glossary_names} ")

    return None

def get_glossary_categories(connection,glossary_id) :
    restapi = f"/glossaries/{glossary_id}/categories"
    url = connection['url'] + restapi
    params = {"excludeEmpty":False,"userCategoriesOnly":False}
    headers = {'X-Requested-With': 'XMLHttpRequest'}

    r = requests.get(url, headers=headers, auth=connection['auth'],params = params)
    response = json.loads(r.text)

    if r.status_code != 200:
        logging.error(f"Get glossary items: {response['message']}")

    return response['categories']

def get_glossary_category_id(categories,category_name) :
    for c in categories :
        if c["name"] == category_name :
            return c["id"], c
    logging.warning(f"Glossary category not found: {category_name}")
    return None

def get_glossary_terms(connection,glossary_id,category_id) :
    restapi = f"/glossaries/{glossary_id}/terms"
    url = connection['url'] + restapi
    params = {"top": 100, "filterUserOnly": False}
    if category_id :
        params["filterCat"] = category_id

    headers = {'X-Requested-With': 'XMLHttpRequest'}

    r = requests.get(url, headers=headers, auth=connection['auth'],params = params)
    response = json.loads(r.text)

    if r.status_code != 200:
        logging.error(f"Get glossary terms: {response['message']}")

    return response


def upload_glossary_term(connection,glossary_id,term) :
    restapi = f"/glossaries/{glossary_id}/terms"
    url = connection['url'] + restapi
    # params = {"hierarchyId":hierarchy_id,"withTags":True}
    headers = {'X-Requested-With': 'XMLHttpRequest','accept': 'application/json'}

    r = requests.post(url, headers=headers, auth=connection['auth'], data = term)
    response = json.loads(r.text)

    if r.status_code != 200:
        logging.error(f"Upload glossaries: {response['message']}")

    return response


#########
# MAIN
########
if __name__ == '__main__' :

    logging.basicConfig(level=logging.DEBUG)

    with open('config_catalog.yaml') as yamls:
        params = yaml.safe_load(yamls)

    glossary_resource = "/app/datahub-app-metadata"
    conn = {'url':params['URL']+glossary_resource,
            'auth':(params['TENANT']+'\\'+ params['USER'],params['PWD'])}
    #data_directory = params['data_directory']

    glossary_name = 'Chemical Industry'
    category_name = 'Company Products'

    glossary_id = get_glossary_id(conn, glossary_name)
    categories = get_glossary_categories(conn,glossary_id)
    print(json.dumps(categories,indent=4))

    category_id, category = get_glossary_category_id(categories,category_name)

    terms = get_glossary_terms(conn,glossary_id,category_id)
    print(json.dumps(terms,indent=4))

    term = {
        "categories":[category],
        "name": "ACCUREL® 98 CM 496",
        "customInputs": [],
        "keywords": [{"key": "ACCUREL", "name": "ACCUREL"}],
        "synonyms":[{"key": "ACCUREL", "name": "ACCUREL"}],
        "description": "ACCUREL® 98 CM 496 is an additive masterbatch of TAICROS® on LLDPE. The active ingredient TAICROS® is a trifunctional monomer which can polymerize in the presence of radicals formed by irradiation or peroxides."}

    upload_glossary_term(conn,glossary_id=glossary_id,term=term)

