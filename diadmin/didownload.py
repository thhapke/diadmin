#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

from os import path,makedirs,getcwd,mkdir
import errno
import logging
import argparse
import re
from subprocess import run
import tarfile

import yaml

from diadmin.vctl_cmds.login import di_login
from diadmin.vctl_cmds.vrep import get_all_files, read_file, export_artifact

VFLOW_PATHS = {'operators':'files/vflow/subengines/com/sap/python36/operators/',
               'pipelines':'files/vflow/graphs/',
               'dockerfiles':'files/vflow/dockerfiles/'}

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

def change_target_dir(artifact_type,members) :
    for tarinfo in members:
        tarinfo.name = re.sub(VFLOW_PATHS[artifact_type],'',tarinfo.name)
        #print(tarinfo.name)
        yield tarinfo

def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    achoices = ['operators','pipelines','dockerfiles']
    description =  "Downloads operators, pipelines to local from SAP Data Intelligence to local file system.\nPre-requiste: vctl."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c','--config', help = 'Specifies yaml-config file',default='config_demo.yaml')
    parser.add_argument('artifact_type', help='Type of artifacts.',choices=achoices)
    parser.add_argument('artifact', help='Artifact name (package, graph or dockerfile)')
    parser.add_argument('-u', '--user', help='SAP Data Intelligence user if different from login-user. Not applicable for solutions-download')
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

    user = params['USER']
    if  args.user :
        user = args.user

    if not path.isdir(args.artifact_type) :
        mkdir(args.artifact_type)

    target = path.join(args.artifact_type,args.artifact + '.tgz')
    export_artifact(args.artifact_type,args.artifact,target,user)
    target = path.join('.',target)
    with tarfile.open(target) as tar:
        logging.info(f'Extract \'{target}\' to: {args.artifact_type}')
        tar.extractall(path=args.artifact_type,members=change_target_dir(args.artifact_type,tar))


if __name__ == '__main__':
    main()