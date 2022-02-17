#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
import urllib.parse

import requests
import json
import re

import logging
import yaml


### RULES
def get_rules(connection,rulebook_id=None) :

    restapi = f"/rules"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = ''
    if rulebook_id:
        params = {"rulebookId":rulebook_id}

    r = requests.get(url, headers=headers, auth=connection['auth'],params=params)
    response = json.loads(r.text)

    if r.status_code != 200:
        logging.error(f"GET Rules: {response['message']}")

    return response

def condense_rules(rules_dict) :

    rules = list()
    for c in rules_dict['categories'] :
        for r in c['rules'] :
            rules.append({'id':r['id'],'name':r['name'],'title':r['displayName'],'description':r['description'],
                    'category': c['name'],'num_references':r['numRef'],'status':r['status'],
                    'parameters':r['config']['parameters'],'conditions':r['config']['conditions'],'condition_grouping':r['config']['conditionGrouping'],
                    'filters':r['config']['filters'],'filter_grouping':r['config']['filterGrouping'],'filter_expression':r['config']['filterExpression'],
                    'rule_expression':r['config']['ruleExpression']
                    })

    return rules

def flat_rules(rules_list) :
    flat_rules = list()
    for r in rules_list :
        nr = dict(r)
        nr['parameters'] = json.dumps(nr['parameters'])
        nr['conditions'] = json.dumps(nr['conditions'])
        nr['condition_grouping'] = json.dumps(nr['condition_grouping'])
        nr['filters'] = json.dumps(nr['filters'])
        nr['filter_grouping'] = json.dumps(nr['filter_grouping'])
        nr['filter_expression'] = json.dumps(nr['filter_expression'])
        nr['rule_expression'] = json.dumps(nr['rule_expression'])
        flat_rules.append(nr)

    return flat_rules

### RULEBOOK
def get_rulebooks(connection):
    """Calls API for getting rulebooks
    Args:
         connection (dict): DI connection details
    Returns:
        rulebooks (dict)
    """
    restapi = f"/rules/rulebooks"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = ''

    r = requests.get(url, headers=headers, auth=connection['auth'],params=params)
    response = json.loads(r.text)

    if r.status_code != 200:
        logging.error(f"GET Rulebooks: {response['message']}")

    return response

def get_rulebook_id(connection,rulebook_name) -> str:
    """Retrieves Rulebook id from rulebook name (regex).
        Args:
            connection (dict): DI connection details
        Returns:
        rulebook_id, rulebook_name or none
    """
    rulebooks = get_rulebooks(connection)
    for r in rulebooks['rulebooks'] :
        if re.match(rulebook_name,r['name']):
            return r['id'],r['name']
    logging.error(f'Rulebook not found: {rulebook_name}')
    return None,None

def condense_rulebooks(rulebooks_dict) :
    """Gets extracts the essential data from original json and flattens data structure.
    :arg: dict of api structure"""

    flat_rulebooks = list()
    for c in rulebooks_dict['rulebooks'] :
        flat_rulebooks.append({'id':c['id'],'name':c['name'],'description':c['description'],
                               'last_update':c['lastModifiedDate'],'num_rules': c['numRules'],'num_runs':c['numRuns'],
                               'status':c['status'],'error_threshold':c['scoreRange']['errorThreshold'],
                               'pass_threshold':c['scoreRange']['passThreshold'],'passed_percent':c['passPercent']})

    return flat_rulebooks

def get_rulebook_results(connection,rulebook_id) :
    """Calls API for getting rulebook results
    Args:
        connection (dict): Contains credentials of DI connections
        rulebook_id (string): DI identfier for rulesbook
    Returns:
        Dict with GET result
    """
    restapi = f"/rules/rulebooks"
    url = connection['url'] + restapi+'/'+str(rulebook_id)+'/datasetResults'
    headers = {'X-Requested-With': 'XMLHttpRequest'}

    r = requests.get(url, headers=headers, auth=connection['auth'])
    response = json.loads(r.text)

    if r.status_code != 200:
        logging.error(f"GET Rulebooks: {response['message']}")

    return response


def flat_results(results,rulebook_id) :
    flat_results = list()
    for r in results['results'] :
        for d in r['datasets']:
            nr = {'id':rulebook_id,'name':results['name'],'description':results['description'],
                  'start_time':r['startTime'],'end_time':r['endTime'],'status':r['status'],
                  'dataset':d['qualifiedName'],'dataset_id':d['datasetId'],
                  'num_records':d['recordCount'],'passed':d['recordsPass'],'failed':d['recordsFailed'],
                  'passed_percent':d['passPercent'],'failed_percent':d['failPercent'],
                  'error_threshold':d['scoreRange']['errorThreshold'],'passed_threshold':d['scoreRange']['passThreshold']}

            flat_results.append(nr)

    return flat_results

# Start rulebook
def start_rulebook(connection,rulebook_id,save_failed_records=True,mode='append'):
    """Calls (inofficial) API for starting a  rulebook
    Args:
        connection (dict): Contains credentials of DI connections
        rulebook_id (string): DI identfier for rulesbook
        save_failed_records: True or False
        mode: append or replace
    Returns:
        Dict with GET result
    """
    if re.search('/api/v1$',connection['url']) :
        url = re.sub('/api/v1$','',connection['url'])
    else :
        raise ValueError('URL has not API (/api/v1) ending: ' + connection['url'])

    restapi = f"/rules/rulebooks"
    url = url + restapi+'/'+str(rulebook_id)+'/execution'
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    data = {"saveFailedRecords":save_failed_records,'failedRecordMode':mode}

    r = requests.post(url, headers=headers, auth=connection['auth'],data=data)
    response = json.loads(r.text)

    if  r.status_code != 202:
        logging.error(f"START Rulebooks: {response['message']}")

    return response

# Get Preparations
def get_preparations(connection):
    """Calls (inofficial) API for getting rulebooks
    Args:
         connection (dict): DI connection details
    Returns:
        rulebooks (dict)
    """
    parsed_url = urllib.parse.urlparse(connection['url'])
    url = 'https://' + parsed_url.hostname + '/app/datahub-app-preparation/preparations'

    headers = {'X-Requested-With': 'XMLHttpRequest'}
    logging.info(f'GET Preparations with RestAPI? {url}')
    r = requests.get(url, headers=headers, auth=connection['auth'])
    response = json.loads(r.text)

    if r.status_code != 200:
        logging.error(f"GET preparations: {response['message']}")
        return None

    return [{'id':p['id'],'name':p['name'],'description':p['description'],'type':p['type']} for p in response]


def get_preparation_id(connection,preparation_name) -> str:
    """Retrieves Rulebook id from rulebook name (regex).
        Args:
            connection (dict): DI connection details
        Returns:
        rulebook_id, rulebook_name or none
    """
    preparations = get_preparations(connection)
    for p in preparations:
        if re.match(preparation_name,p['name']):
            return p['id'],p['name']
    logging.error(f'Preparation not found: {preparation_name}')
    return None,None


# Start rulebook
def start_preparation(connection,preparation_id,connection_id,path):
    """Calls (inofficial) API for getting starting preparation
    Args:
        connection (dict): Contains credentials of DI connections
        preparation_id (string): DI identfier for preparation
    Returns:
        Dict with GET result
    """
    parsed_url = urllib.parse.urlparse(connection['url'])
    url = 'https://' + parsed_url.hostname + '/app/datahub-app-preparation/v2/preparations/' + preparation_id+'/executions'

    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {"connectionId": connection_id,
            "fromFolder": False,
            "path": path,
            "type": "FILE.CSV",
            "writeMode": "OVERWRITE"}
    #data = json.dumps(data)

    r = requests.get(url, headers=headers, auth=connection['auth'],params=params)
    response = json.loads(r.text)

    if  r.status_code != 200:
        logging.error(f"START Preparation: {response['message']}")

    return response


#########
# MAIN
########

def main() :
    logging.basicConfig(level=logging.INFO)

    with open('config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    conn = {'url':params['URL']+'/app/datahub-app-metadata/api/v1',
            'auth':(params['TENANT']+'\\'+ params['USER'],params['PWD'])}

    RULES = False
    if RULES :
        rules = get_rules(conn)
        rules = condense_rules(rules)
        rules = flat_rules(rules)
        print(json.dumps(rules,indent=4))

    RULEBOOKS = False
    if RULEBOOKS:
        rulebooks = get_rulebooks(conn)
        rulebooks = condense_rulebooks(rulebooks)
        print(json.dumps(rulebooks,indent=4))

    RULEBOOK_RESULTS = False
    if RULEBOOK_RESULTS :
        #rulebook_id = 'ad130320cb31b3c8170033aef772e8f5'
        rulebook_id, rulebook_name = get_rulebook_id(conn,'Pharma Claims')
        rresults = get_rulebook_results(conn,rulebook_id)
        rresults = flat_results(rresults,rulebook_id)
        print(json.dumps(rresults,indent=4))

    RULEBOOK_START = False
    if RULEBOOK_START :
        #rulebook_id = 'ad130320cb31b3c8170033aef772e8f5'
        rulebook_id, rulebook_name = get_rulebook_id(conn,'Pharma Claims')
        response = start_rulebook(conn,rulebook_id,save_failed_records=False)
        print(response)

    PREPARATIONS = True
    if PREPARATIONS :
        prep_id, prep_name = get_preparation_id(conn,"Fast_")
        start_preparation(conn,prep_id,connection_id='S3_catalog',path='/dqm/prepared/'+prep_name+'.csv')
        print(f'Prep name: {prep_name}  ID: {prep_id}')

if __name__ == '__main__' :
    main()