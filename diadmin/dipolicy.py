#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

import json
from os.path import join, exists, isfile, isdir, dirname, basename
from os import getcwd, mkdir, listdir
import logging
import re
import shutil
from zipfile import ZipFile

import argparse

import yaml
import matplotlib.pyplot as plt

from diadmin.vctl_cmds.login import di_login
from diadmin.vctl_cmds.policy import get_policy_resources, get_all_policies, create_policy
from diadmin.analysis.graph_policies import *

PRINT_RESOURCES = False

def rename_policy(policy,newname) :
    policy['id'] = re.sub('^mycompany.',newname+'.',policy['id'],count=1)
    for r in policy["inheritedResources"] :
        r['policyId'] = re.sub('^mycompany.',newname+'.',r['policyId'],count=1)
    for r in policy["policyReferences"] :
        r['id'] = re.sub('^mycompany.',newname+'.',r['id'],count=1)


def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  "Policy utility script for SAP Data Intelligence.\nPre-requiste: vctl."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c',
                        '--config', help = 'Specifies yaml-config file')
    parser.add_argument('-g', '--generate', help='Generates config_demo.yaml file',action='store_true')
    parser.add_argument('-d', '--download', help='Download specified policy. If wildcard \'*\' is used then policies are filtered or all downloaded.')
    parser.add_argument('-u', '--upload', help='Upload new policy (path). If path is directory all json-files uploaded. If path is a pattern like \'policies/mycompany.\' all matching json-files are uploaded.')
    parser.add_argument('-m', '--mycompany', help='Replaces mycompany in policy name.')
    parser.add_argument('-z', '--zip', help='Zip policies',action='store_true')
    parser.add_argument('-f', '--file', help='File to analyse policy structure. If not given all policies are newly downloaded.')
    parser.add_argument('-a', '--analyse', help='Analyses the policy structure. Resource list is saved as \'resources.csv\'.',action='store_true')
    args = parser.parse_args()

    if args.config:
        config_file = args.config
        if not re.match('.+\.yaml',config_file) :
            config_file += '.yaml'
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


    mycompany = args.mycompany if args.mycompany else None

    if args.download :
        if args.download == 'all' or (len(args.download) == 1 and args.download[0] == '*') :
            logging.info('Download all policies!')
            pr = get_all_policies()
        elif args.download[-1] == '*' :
            logging.info(f"Download policy {args.download}")
            pr = get_all_policies(filter = args.download)
        else :
            logging.info(f"Download policy {args.download}")
            p = get_policy_resources(args.download)
            pr = list()
            pr.append(p)

        # rename policy-name
        if mycompany :
            for p in pr :
                rename_policy(p,mycompany)

        # Summary of resources for stdout
        if PRINT_RESOURCES :
            resources = list()

            for p in pr :
                print(f"Direct {p['id']}:")
                for r in p['resources'] :
                    pstr = f"{r['resourceType']} - {r['contentData']['activity']}"
                    if 'name' in r['contentData'] :
                        pstr += f"- {r['contentData']['name']}"
                    if 'connectionId' in r['contentData'] :
                        pstr += f" - {r['contentData']['connectionId']}"

                    print(pstr)
                print('Inherited->')
                for ip in p['inheritedResources'] :
                    for r in ip['resources']:
                        pstr = f"{ip['policyId']} - {r['resourceType']} - {r['contentData']['activity']}"
                        if 'name' in r['contentData'] :
                            pstr += f"- {r['contentData']['name']}"
                        if 'connectionId' in r['contentData'] :
                            pstr += f" - {r['contentData']['connectionId']}"
                        print(pstr)

        # Saving policy
        policy_files = list()
        for p in pr :
            filename = join(params['POLICIES_PATH'],p['id'] + '.json')
            policy_files.append(filename)
            with open(filename, 'w') as json_file:
                json.dump(p,json_file,indent=4)

        if args.zip :
            zipfilename = join(params['POLICIES_PATH'],'policies.zip')
            logging.info(f"Write zip-file: {zipfilename}")
            with ZipFile(zipfilename, 'w') as zipfile :
                for pf in policy_files :
                    zipfile.write(pf)


    if args.upload :
        upload_files = list()
        # all file in directory
        if isdir(args.upload) :
            logging.info(f"Upload directory: {args.upload}")
            upload_files = [join(args.upload,f) for f in listdir(args.upload) if isfile(join(args.upload,f))
                            and re.match('.+\.json$',f) and not f == 'policies.json']
        elif args.upload == 'all' or args.upload == '*' :
            logging.info(f"Upload directory: {params['POLICIES_PATH']}")
            upload_files = [join(params['POLICIES_PATH'],f) for f in listdir(params['POLICIES_PATH']) \
                            if isfile(join(params['POLICIES_PATH'],f))
                            and re.match('.+\.json$',f) and not f == 'policies.json']
        else :
            parent_dir = dirname(args.upload)
            bname = basename(args.upload)
            upload_files = [join(parent_dir,f) for f in listdir(parent_dir) \
                            if isfile(join(parent_dir,f)) and re.match('.+\.json$',f)
                            and re.match(bname,f) and not f == 'policies.json' ]

        if mycompany :
            new_uploadfiles = list()
            for f in upload_files :
                parent_dir = dirname(f)
                filename = basename(f)
                new_filename = re.sub('mycompany.',mycompany+'.',filename)
                logging.info(f"Copy policy: {filename} - {new_filename}")
                with open(f, 'r') as json_file:
                    policies_dict = json.load(json_file)
                    rename_policy(policies_dict,mycompany)
                    with open(join(parent_dir,new_filename),'w') as json_file :
                        json.dump(policies_dict,json_file,indent=4)
                new_uploadfiles.append(join(parent_dir,new_filename))
            upload_files = new_uploadfiles

        basic_files = list()
        nonbasic_files = list()
        failed_files = list()
        for f in upload_files :
            with open(f, 'r') as pfile:
                p = json.load(pfile)
                if len(p["inheritedResources"]) == 0 :
                    basic_files.append(f)
                else :
                    nonbasic_files.append(f)
        count = 1
        for f in basic_files :
            logging.info(f'Uploading policy: {f} ({count}/{len(upload_files)})')
            ret = create_policy(f)
            if ret:
                failed_files.append(f)
            count +=1
        for f in nonbasic_files :
            logging.info(f'Uploading policy: {f} ({count}/{len(upload_files)})')
            ret = create_policy(f)
            if ret:
                failed_files.append(f)
            count +=1

        if len(failed_files) > 0 :
            fstr = '\n'.join(failed_files)
            logging.warning(f"WARNING not all policies could be uploaded:\n{fstr}")

    if args.generate :
        logging.info(f'Generates config_demo.yaml and stores to {config_file} ')
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
            filename = join(params['POLICIES_PATH'],args.file)
            logging.info(f"Read analysis from: {filename}")
            with open(filename, 'r') as json_file:
                policies_dict = json.load(json_file)
        else :
            policies_dict = get_all_policies()
            filename = join(params['POLICIES_PATH'],'policies.json')
            with open(filename, 'w') as json_file:
                json.dump(policies_dict,json_file,indent=4)

        G = build_network(policies_dict, resource_classes)
        tag_nodes_by_name(G,params['POLICY_FILTER'],params['POLICY_FILTER'],successor_nodes=True,remove_untagged=True)

        compute_levels(graph=G)
        resources = add_inherited_resources(G)
        resources = check_duplicate_resources(resources)
        resources = classify_policy(G, resources,params['CLASS_THRESHOLD'])
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
            fnodes = filter_nodes(G, att_filter={'path_node':True})
            str_nodes = '\n'.join(fnodes)
            txt_file.write(str_nodes)

if __name__ == '__main__':
    main()