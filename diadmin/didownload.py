#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

from os import path,makedirs,getcwd
import errno
import logging
import argparse
import re
from subprocess import run

import yaml

from diadmin.vctl_cmds.login import di_login
from diadmin.vctl_cmds.vrep import get_all_files, read_file

VFLOW_PATHS = {'operators':'/files/vflow/subengines/com/sap/python36/operators/',
               'pipelines':'/files/vflow/graphs/',
               'dockerfiles':'/files/vflow/dockerfiles/'}

def save_open(file):
    parentpath = path.dirname(file)
    try:
        makedirs(parentpath)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and path.isdir(parentpath):
            pass
        else: raise
    return open(file, 'w')

def download(artifact,artifact_type='operator') :

    apath = VFLOW_PATHS[artifact_type]
    if artifact == '.' :
        artifact_path = apath
    else :
        artifact_path = path.join(apath,artifact.replace('.','/'))
    logging.info(f'Download {artifact_type}: {artifact_path}')
    files = get_all_files(artifact_path)

    for f in files :
        content = read_file(f)
        art_path = re.sub(f'^{apath}','',f)
        art_path = art_path.replace('/',path.sep)
        rpath =  path.join(getcwd(),artifact_type,art_path)

        with save_open(rpath) as fp :
            logging.info(f'Save file: {rpath}')
            fp.write(content)



def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  "Downloads operators, pipelines to local from SAP Data Intelligence to local file system.\nPre-requiste: vctl."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c','--config', help = 'Specifies yaml-config file',default='config_demo.yaml')
    parser.add_argument('-o', '--operator', help='Downloads operators from operators-folder')
    parser.add_argument('-p', '--pipeline', help='Downloads pipelines from graphs-folder ')
    parser.add_argument('-d', '--dockerfile', help='Downloads dockerfiles from dockerfiles-folder')
    parser.add_argument('-g', '--gitcommit', help='Git commit for the downloaded files',action='store_true')
    args = parser.parse_args()

    config_file = 'config.yaml'
    if args.config:
        config_file = args.config
    with open(config_file) as yamls:
        params = yaml.safe_load(yamls)

    ret = di_login(params)
    if not ret == 0 :
        return ret

    if args.operator :
        download(args.operator,'operators')
        if args.gitcommit :
            folder = path.join('operators',args.operator.replace('.','/'))
            ret = run(['git','add','-f',folder])
            ret = run(['git','commit','-m','di changes',folder])
    elif args.pipeline :
        download(args.pipeline,'pipelines')
        if args.gitcommit :
            folder = path.join('pipelines',args.pipeline.replace('.','/'))
            ret = run(['git','add','-f',folder])
            ret = run(['git','commit','-m','di changes',folder])
    elif args.dockerfile :
        download(args.dockerfile,'dockerfiles')
        if args.gitcommit :
            folder = path.join('dockerfiles',args.dockerfile.replace('.','/'))
            ret = run(['git','add','-f',folder])
            ret = run(['git','commit','-m','di changes',folder])
    else:
        print('Error: Missing artifact type [-o,-p,-d]')
        return -1


if __name__ == '__main__':
    main()