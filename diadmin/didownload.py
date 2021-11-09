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

from diadmin.utils.utils import add_defaultsuffix, toggle_mockapi
from diadmin.vctl_cmds.login import di_login
from diadmin.vctl_cmds.vrep import get_all_files, read_file, export_artifact, solution_from_repo
from diadmin.vctl_cmds.solution import download_solution

VFLOW_PATHS = {'operators':'files/vflow/subengines/com/sap/python36/operators/',
               'graphs':'files/vflow/graphs/',
               'dockerfiles':'files/vflow/dockerfiles/'}



def change_target_dir(artifact_type,members) :
    for tarinfo in members:
        tarinfo.name = path.relpath(tarinfo.name,VFLOW_PATHS[artifact_type])
        #tarinfo.name = re.sub(VFLOW_PATHS[artifact_type],'',tarinfo.name)
        if not tarinfo.name  == '.':
            yield tarinfo

def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    achoices = ['operators','graphs','dockerfiles','menu','all','*','solution']
    description =  "Downloads operators, pipelines or solution to local from SAP Data Intelligence to local file system.\nPre-requiste: vctl."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c','--config', help = 'Specifies yaml-config file',default='config_demo.yaml')
    parser.add_argument('-i','--init', help = 'Creates a config.yaml and the necessary folders. Additionally you need '
                                              'to add \'* *\' as dummy positional arguments',action='store_true')
    parser.add_argument('artifact_type', help='Type of artifacts.',choices=achoices)
    parser.add_argument('artifact', help='Artifact name of package, graph or dockerfile or wildcard \'*\'. For \'all\' wildcard is required.',default='*')
    parser.add_argument('-n', '--solution', help='Solution imported to vrep before artifacts downloaded.')
    parser.add_argument('-v', '--version', help='Version of solution. Required for option --solution')
    parser.add_argument('-u', '--user', help='SAP Data Intelligence user if different from login-user. Not applicable for solutions-download')
    parser.add_argument('-g', '--gitcommit', help='Git commit for the downloaded files',action='store_true')
    args = parser.parse_args()

    if args.init :
        logging.info('Creating config-file: config.yaml')
        for f in  VFLOW_PATHS.keys() :
            if not path.isdir(f) :
                mkdir(f)
        with open("config.yaml",'w') as file :
            params = {'URL': 'https://vsystem.ingress.xxx.shoot.live.k8s-hana.ondemand.com',
                      'TENANT' : 'default',
                      'USER':'user',
                      'PWD':'pwd123'}
            yaml.dump(params,file)
        return 0

    config_file = 'config.yaml'
    if args.config:
        config_file = add_defaultsuffix(args.config,'yaml')

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

    if args.solution:
        solution_from_repo(args.solution, args.version)

    if args.artifact_type == 'solution' :
        file = path.join('solutions',args.artifact + '.zip')
        download_solution(args.artifact,args.version)
    elif args.artifact_type == 'menu_panel' :
        pass
    else :
        target = [args.artifact_type]
        if args.artifact_type == 'all' :
            target = [('operators','operators/opertator.tgz'),
                      ('graphs','graphs/graphs.tgz'),
                      ('dockerfiles','dockerfiles/dockerfiles.tgz')]
        elif args.artifact == '.' or args.artifact == '*':
            target = [(args.artifact_type,path.join(args.artifact_type,args.artifact_type + '.tgz'))]
        else :
            args.artifact = args.artifact.replace('.',path.sep)
            if path.sep in args.artifact :
                parentdir = args.artifact.split(path.sep)[0]
                if not path.isdir(path.join(args.artifact_type,parentdir)):
                    mkdir(path.join(args.artifact_type,parentdir))

            target = [(args.artifact_type,path.join(args.artifact_type,args.artifact + '.tgz'))]

        for t in target :
            export_artifact(t[0],t[0],t[1],user)
            with tarfile.open(t[1]) as tar:
                logging.info(f'Extract \'{t[1]}\' to: {t[0]}')
                tar.extractall(path=t[0],members=change_target_dir(t[0],tar))

                #toggle_mockapi(comment=False)

    if args.gitcommit :

        folder = path.join(args.artifact_type,args.artifact)
        logging.info(f'Auto-commit of downloaded artifact: {folder}')
        run(['git','add',folder])
        run(['git','commit','-m','didownload'])

if __name__ == '__main__':
    main()