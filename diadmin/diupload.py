#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

from os import path,makedirs,getcwd, walk, listdir,mkdir
import logging
import argparse
import tarfile
import re
from subprocess import run

import yaml

from diadmin.vctl_cmds.login import di_login
from diadmin.vctl_cmds.vrep import import_artifact, solution_to_repo

VFLOW_PATHS = {'operators':'/files/vflow/subengines/com/sap/python36/',
               'graphs':'/files/vflow/',
               'dockerfiles':'/files/vflow/'}


def make_tarfile(artifact_type,source) :
    if artifact_type == 'all' or artifact_type == '*' :
        sources =  [('operators','operators'),('graphs','graphs'),('dockerfiles','dockerfiles')]
    elif source == '.' or source == '*' :
        sources = [(artifact_type,artifact_type)]
    else :
        sources =[(artifact_type,path.join(artifact_type,source))]
    tar_filename = path.join(artifact_type,source + '.tgz')
    with tarfile.open(tar_filename, "w:gz") as tar:
        for s in sources :
            for d in listdir(s[1]) :
                sd = path.join(s[1],d)
                if path.isdir(sd) :
                    tar.add(sd)
    return tar_filename

def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    achoices = ['operators','graphs','dockerfiles','all','*']
    description =  "Uploads operators, graphs, dockerfiles and bundle to SAP Data Intelligence.\nPre-requiste: vctl."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-i','--init', help = 'Creates a config.yaml and the necessary folders. Additionally you need '
                                              'to add \'* *\' as dummy positional arguments',action='store_true')
    parser.add_argument('-c','--config', help = 'Specifies yaml-config file',default='config_demo.yaml')
    parser.add_argument('-r','--conflict', help = 'Conflict handling flag of \'vctl vrep import\'')
    parser.add_argument('artifact_type', help='Type of artifacts. \'bundle\'- only supports .tgz-files with differnt artifact types.',choices=achoices)
    parser.add_argument('artifact', help='Artifact file(tgz) or directory')
    parser.add_argument('-n', '--solution', help='Solution name if uploaded artificats should be exported to solution repository as well.')
    parser.add_argument('-s', '--description', help='Description string for solution.')
    parser.add_argument('-v', '--version', help='Version of solution. Necessary if exported to solution repository.',default='0.0.1')
    parser.add_argument('-u', '--user', help='SAP Data Intelligence user if different from login-user. Not applicable for solutions-upload')
    parser.add_argument('-g', '--gitcommit', help='Git commit for the uploaded files',action='store_true')
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
        config_file = args.config
    with open(config_file) as yamls:
        params = yaml.safe_load(yamls)

    ret = 0
    ret = di_login(params)
    if not ret == 0 :
        return ret

    user = params['USER']
    if  args.user :
        user = args.user

    conflict = None
    if args.conflict :
        conflict = args.conflict

    if (re.match('.+\.tgz$',args.artifact) or re.match('.+\.tar.gz$',args.artifact)):
        import_artifact(args.artifact_type,args.artifact,user,conflict)
    else :
        tf = make_tarfile(args.artifact_type,args.artifact)
        import_artifact(args.artifact_type,tf,user,conflict)

    if args.solution:
        if re.match('.+\.tgz$',args.artifact):
            basename =  re.match('(.+)\.tgz$',args.artifact).group(0)
        elif re.match('.+\.tar.gz$',args.artifact) :
            basename = re.match('(.+)\.tar.gz$',args.artifact).group(0)
        elif args.artifact == '*' or args.artifact == '.' :
            basename = args.artifact_type
        else :
            basename = args.artifact
        basename = path.basename(basename)
        source = path.join(VFLOW_PATHS[args.artifact_type],basename)
        solution_to_repo(source, args.solution, args.version, args.description)

    if args.gitcommit :
        folder = path.join(args.artifact_type,args.artifact)
        logging.info(f'Auto-commit for uploaded artifact: {folder}')
        run(['git','add',folder])
        run(['git','commit','-m','diupload'])


if __name__ == '__main__':
    main()