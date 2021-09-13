#!/usr/bin/python3
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
from policies import *

def main() :

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
                  'OUTPUT_PATH': '../data',
                  'RESOURCE_CLASSES':get_default_resource_classes()}

        with open(config_file, 'w') as outfile:
            yaml.dump(params, outfile, default_flow_style=False)
        sys.exit(1)

    with open(config_file) as yamls:
        logging.info(f"Reads config file: {config_file}")
        params = yaml.safe_load(yamls)

    if 'RESOURCE_CLASSES' in params :
        resource_classes = params['RESOURCE_CLASSES']
    else :
        resource_classes = get_default_resource_classes()

    if 'COLOR_MAP' in params :
        color_map = params['COLOR_MAP']
    else :
        rclasses = set(resource_classes.values())
        rclasses.update(['multiple'])
        cmap = plt.get_cmap('Set1')
        color_map = {r:cmap.colors[i] for i,r in enumerate(rclasses)}

    if args.file :
        filename = join(params['OUTPUT_PATH'],'policies.json')
        logging.info(f"Read policies from: {filename}")
        with open(filename, 'r') as json_file:
            policies_dict = json.load(json_file)
    else :
        logging.info(f"Login to {params['URL']}")
        di_login(params)
        logging.info('Download policy details')
        policies_dict = get_all_policies()
        filename = join(params['OUTPUT_PATH'],'policies.json')
        with open(filename, 'w') as json_file:
            json.dump(policies_dict,json_file,indent=4)


    G = build_network(policies_dict,resource_classes)

    resources = add_inherited_resources(G)
    resources = check_duplicate_resources(resources)
    resources = classify_policy(G,resources)


    # DRAWING
    draw_graph(G,resource_classes,color_map,filename=join(params['OUTPUT_PATH'],'policies.png'))

    #SAVING

    filename = join(params['OUTPUT_PATH'],'resources.csv')
    save_resources(filename,resources)

    filename = join(params['OUTPUT_PATH'],'chart_policy_ids.csv')
    with open(filename, 'w') as txt_file:
        logging.info(f"Save policyIDs used in graph to \"{filename}\"")
        str_nodes = print_nodes(G, att_filter={'path_node':True}, only_id=True)
        txt_file.write(str_nodes)

if __name__ == '__main__':
    main()