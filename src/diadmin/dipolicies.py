#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

import argparse
import logging
import sys
from os.path import join
import json

import yaml

from login import di_login
from policies import \
    get_all_policies, \
    build_network, \
    draw_graph, \
    print_nodes, \
    add_resources, \
    check_duplicate_resources, \
    save_resources

#
# command line args
#
desctiption =  """"Dowloads SAP Data Intelligence policy details to policy_details.json. 
Policy network is saved as chart to policies.png. Pre-requiste: vctl.
Download vctl: https://launchpad.support.sap.com/#/softwarecenter/template/products/%20_APP=00200682500000001943&_EVENT=DISPHIER&HEADER=Y&FUNCTIONBAR=N&EVENT=TREE&NE=NAVIGATE&ENR=73554900100800002981&V=INST&TA=ACTUAL&PAGE=SEARCH/DATA%20INTELLIGENCE-SYS%20MGMT%20CLI"
"""
parser = argparse.ArgumentParser(description=desctiption)
parser.add_argument('-c','--config', help = 'Specifies yaml-config file')
parser.add_argument('-g', '--generate', help='Generates config.yaml file',action='store_true')
parser.add_argument('-f', '--file', help='Reads \"policies.json\"-file instead of downloading it.',action='store_true' )
args = parser.parse_args()

# Logging
logging.basicConfig(format='%(message)s', level=logging.INFO)

config_file = 'config.yaml'
if args.config:
    config_file = args.config

if args.generate :
    logging.info(f'Generates config.yaml and stores to {config_file} ')
    params = {'URL' : 'https://demo.k8s-hana.ondemand.com',
              'TENANT': 'default',
              'USER' : 'demo',
              'PWD' : 'demo123',
              'OUTPUT_PATH': '../data'}

    with open(config_file, 'w') as outfile:
        yaml.dump(params, outfile, default_flow_style=False)
    sys.exit(1)


with open(config_file) as yamls:
    logging.info(f"Reads config file: {config_file}")
    params = yaml.safe_load(yamls)

if args.file :
    filename = join(params['OUTPUT_PATH'],'policies.json')
    logging.info(f"Read policies from: {filename}")
    with open(filename, 'r') as json_file:
        policies = json.load(json_file)
else :
    logging.info(f"Login to {params['URL']}")
    di_login(params)
    logging.info('Download policy details')
    policies = get_all_policies()
    filename = join(params['OUTPUT_PATH'],'policies.json')
    with open(filename, 'w') as json_file:
        json.dump(policies,json_file,indent=4)


G = build_network(policies)
draw_graph(G,filename=join(params['OUTPUT_PATH'],'policies.png'))

filename = join(params['OUTPUT_PATH'],'chart_policy_ids.csv')
with open(filename, 'w') as txt_file:
    str_nodes = print_nodes(G, att_filter={'path_node':True}, only_id=True)
    txt_file.write(str_nodes)

logging.info("Add resources to graph and accrue inherited resources")
resources = add_resources(G,policies)
logging.info("Check for duplicates")
resources = check_duplicate_resources(resources)

filename = join(params['OUTPUT_PATH'],'resources.csv')
logging.info("Save resources to \"resources.csv\"")
save_resources(filename,resources)

