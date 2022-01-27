#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

import logging
import json
import shutil
import re
from os import getcwd, remove
from os.path import basename

from subprocess import check_output, run

def get_policy(policy_id) :
    policies_cmd = ['vctl','policy','policy',policy_id]
    policy = check_output(policies_cmd).decode('utf-8')
    return policy

def get_list_policies():
    list_policies_cmd = ['vctl','policy','list-policies','--format','json']
    list_policies = check_output(list_policies_cmd).decode('utf-8')
    return json.loads(list_policies)

def get_policy_resources(policy_id) :
    policies_cmd = ['vctl','policy','get',policy_id,'--format','json']
    policy = json.loads(check_output(policies_cmd).decode('utf-8'))

    return policy

def get_policy_list_assignments(user=None,format='json') :
    if not user :
        assignements_cmd = ['vctl','policy','list-assignments','-a','--format',format]
    else :
        assignements_cmd = ['vctl','policy','list-assignments',user,'--format',format]
    assignements = policy = json.loads(check_output(assignements_cmd).decode('utf-8'))
    return assignements


def get_resources() :
    resources_cmd = ['vctl','policy','resources']
    res = check_output(resources_cmd).decode('utf-8').split('\n')
    resources = list()
    result = list()
    for r in  res :
        rec = r.split()
        if len(rec) and not rec[0] == 'ID' and not rec[1] == 'Visibility' :
            result.append({'ID':rec[0],'Visibility':rec[1]})
            resources.append(rec[0])
    logging.info(f'Number of Resources: {len(resources)}')
    return result, resources


def get_all_policies(filter = None) :
    '''
    Getting all analysis with details using vctl
    :return: None
    '''
    logging.info('Download all policy details')
    policies = get_list_policies()

    pd_list = list()
    num_id = 1
    for p in policies :
        if filter and not re.match(filter[:-1],p['id']) :
            continue

        logging.info(f"Get details: {p['id']}")
        policy = get_policy_resources(p['id'])
        policy['num_id'] = num_id
        pd_list.append(policy)
        num_id +=1

    # add integer id to each policy for easing later analysis

    return pd_list

def create_policy(filename) :
    copy_f = shutil.copy(filename,getcwd())
    basename_copy_f = basename(copy_f)
    create_policy_cmd = ['vctl','policy','create',basename_copy_f]
    logging.info(f"cmd: {' '.join(create_policy_cmd)}")
    compproc = run(create_policy_cmd)
    remove(copy_f)
    return compproc.returncode

