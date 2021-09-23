#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

import json
from os.path import join, exists
from os import getcwd, mkdir
import logging

import argparse

import yaml
import matplotlib.pyplot as plt

from vctl_cmds.login import di_login
from vctl_cmds.policy import get_policy_resources, get_all_policies, create_policy
from analysis.graph_policies import *


def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  """"Policy utility script for SAP Data Intelligence.\nPre-requiste: vctl.
    Download vctl: https://launchpad.support.sap.com/#/softwarecenter/template/products/%20_APP=00200682500000001943&_EVENT=DISPHIER&HEADER=Y&FUNCTIONBAR=N&EVENT=TREE&NE=NAVIGATE&ENR=73554900100800002981&V=INST&TA=ACTUAL&PAGE=SEARCH/DATA%20INTELLIGENCE-SYS%20MGMT%20CLI"
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c','--config', help = 'Specifies yaml-config file',default='config.yamls')
    parser.add_argument('-g', '--generate', help='Generates config.yaml file',action='store_true')
    parser.add_argument('-d', '--download', help='Download specified policy. If \'all\' then all policies are download')
    parser.add_argument('-u', '--upload', help='Upload new policy.')
    parser.add_argument('-f', '--file', help='File to analyse policy structure. If not given all policies are newly downloaded.')
    parser.add_argument('-a', '--analyse', help='Analyses the policy structure. Resource list is saved as \'resources.csv\'.',action='store_true')
    args = parser.parse_args()

    config_file = 'config.yaml'
    if args.config:
        config_file = args.config
    with open(config_file) as yamls:
        params = yaml.safe_load(yamls)

    # Check if POLICIES_PATH exist
    if not exists(join(getcwd(), params['POLICIES_PATH'])) :
        policies_path = join(getcwd(), params['POLICIES_PATH'])
        logging.info(f"Make policies folder: {params['POLICIES_PATH']} at {getcwd()}")
        mkdir(policies_path)

    if args.download or args.upload or ( args.analyse and not args.file):
        ret = di_login(params)
        if not ret == 0 :
            return ret

    if args.download :
        if args.download == 'all' :
            logging.info('Download all policies!')
            pr = get_all_policies()
        else :
            logging.info(f"Download policy {args.download}")
            pr = get_policy_resources(args.download)

        # Summary of resources for stdout
        resources = list()
        print(f"Direct {pr['id']}:")
        for r in pr['resources'] :
            pstr = f"{r['resourceType']} - {r['contentData']['activity']}"
            if 'name' in r['contentData'] :
                pstr += f"- {r['contentData']['name']}"
            if 'connectionId' in r['contentData'] :
                pstr += f" - {r['contentData']['connectionId']}"

            print(pstr)
        print('Inherited->')
        for ip in pr['inheritedResources'] :
            for r in ip['resources']:
                pstr = f"{ip['policyId']} - {r['resourceType']} - {r['contentData']['activity']}"
                if 'name' in r['contentData'] :
                    pstr += f"- {r['contentData']['name']}"
                if 'connectionId' in r['contentData'] :
                    pstr += f" - {r['contentData']['connectionId']}"
                print(pstr)

        # Saving policy
        filename = join(params['POLICIES_PATH'],pr['id'] + '.json')
        with open(filename, 'w') as json_file:
            json.dump(pr,json_file,indent=4)

    if args.upload :
        logging.info(f"Upload policy: {args.upload}")
        create_policy(args.upload)

    if args.generate :
        logging.info(f'Generates config.yaml and stores to {config_file} ')
        params = {
            'URL' : 'https://demo.k8s-hana.ondemand.com',
            'TENANT': 'default',
            'USER' : 'user',
            'PWD' : 'user123',
            'POLICIES_PATH': '../policies',
            'RESOURCE_CLASSES':get_default_resource_classes(),
            'COLOR_MAP':get_default_color_map()
        }

        with open(config_file, 'w') as outfile:
            yaml.dump(params, outfile, default_flow_style=False)
        return 0

    if args.analyse :
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
            filename = join(params['POLICIES_PATH'],'policies.json')
            logging.info(f"Read analysis from: {filename}")
            with open(filename, 'r') as json_file:
                policies_dict = json.load(json_file)
        else :
            policies_dict = get_all_policies()
            filename = join(params['POLICIES_PATH'],'policies.json')
            with open(filename, 'w') as json_file:
                json.dump(policies_dict,json_file,indent=4)

        G = build_network(policies_dict, resource_classes)

        resources = add_inherited_resources(G)
        resources = check_duplicate_resources(resources)
        resources = classify_policy(G, resources)
        resources = add_path_node_info(G,resources)
        resources = add_node_att(G,resources)

        # DRAWING
        logging.info('Saving chart: policies_chart.png')
        draw_graph(G, resource_classes, color_map, filename=join(params['POLICIES_PATH'], 'policies_chart.png'))

        #SAVING
        filename = join(params['POLICIES_PATH'],'resources.csv')
        save_resources(filename, resources)

        filename = join(params['POLICIES_PATH'],'chart_policy_ids.csv')
        with open(filename, 'w') as txt_file:
            logging.info(f"Save policyIDs used in graph to \"{filename}\"")
            str_nodes = print_nodes(G, att_filter={'path_node':True}, only_id=True)
            txt_file.write(str_nodes)

if __name__ == '__main__':
    main()