#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

from os import path,makedirs,getcwd,mkdir
import logging
import argparse
from subprocess import run
import tarfile

import yaml

from diadmin.utils.utils import add_defaultsuffix, toggle_mockapi,mksubdirs
from diadmin.vctl_cmds.login import di_login
from diadmin.vctl_cmds.vrep import get_all_files, read_file, export_object, solution_from_repo, VFLOW_PATHS
from diadmin.vctl_cmds.solution import download_solution

# global variable
root_dir = '.'

def change_target_dir(object_type,members) :
    for tarinfo in members:
        tarinfo.name = path.relpath(tarinfo.name,VFLOW_PATHS[object_type])
        if not tarinfo.name  == '.':
            yield tarinfo

# List of artifacts to be downloaded with corresponding source and target
# Format (artifact_type,artifact_folder,target_zipfile)
def get_targetlist(object_type, object_name) :
    target = list()
    if object_type == 'all' :
        target = [('operators','operators',root_dir+'/operators/operator.tgz'),
                  ('graphs','graphs',root_dir+'/graphs/graphs.tgz'),
                  ('dockerfiles','dockerfiles',root_dir+'/dockerfiles/dockerfiles.tgz'),
                  ('vtypes','vtypes',root_dir+'/vtypes/vtypes.tgz'),
                  ('general','general',root_dir+'/general/general.tgz')]
    elif object_name == '.' or object_name == '*':
        target = [(object_type, object_type, path.join(root_dir,object_type, object_type + '.tgz'))]
    else :
        object_name = object_name.replace('.', path.sep)
        target = [(object_type, object_name,path.join(root_dir,object_type, object_name + '.tgz'))]
    return target

def main() :

    global root_dir

    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    achoices = ['project','operators','operators_gen2','graphs','dockerfiles','vtypes','menu','all','*','solution']
    description =  "Downloads operators, pipelines, vtypes, menus, dockerfiles or solution from SAP Data Intelligence to local file system.\nPre-requiste: vctl."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c','--config', help = 'Specifies yaml-config file',default='config_demo.yaml')
    parser.add_argument('-i','--init', help = 'Creates a config.yaml and the necessary folders. Additionally you need '
                                              'to add \'* *\' as dummy positional arguments',action='store_true')
    parser.add_argument('object_type', help='Type of development object.',choices=achoices)
    parser.add_argument('object_name', help='DI development object name, incl. \'*\'. For \'all\' wildcard is required.',default='*')
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

    if 'ROOT_DIR' in params :
        root_dir = params['ROOT_DIR']

    ret = di_login(params)
    if not ret == 0 :
        return ret

    user = params['USER']
    if  args.user :
        user = args.user

    if not path.isdir(args.object_type) :
        mkdir(args.object_type)

    if args.solution:
        solution_from_repo(args.solution, args.version)

    if args.object_type == 'solution' :
        file = path.join('solutions',args.object_name + '.zip')
        download_solution(args.object_name,args.version)
    else :
        target_list = list()
        if args.object_type == 'project' :
            project_file = add_defaultsuffix(args.object_name,'yaml')
            with open(path.join('projects',project_file)) as yamls:
                project = yaml.safe_load(yamls)
            for object_type in project :
                if object_type in ['graphs','operators','operators_gen2','dockerfiles','vtypes'] and not project[object_type] == None:
                    for artifact in project[object_type] :
                        target_list += get_targetlist(object_type,artifact)
                else :
                    logging.error(f'Unknown project item: {object_type}')
        else :
            target_list = get_targetlist(args.object_type,args.object_name)

        for t in target_list :
            mksubdirs('.',t[2])
            export_object(t[0], t[1], t[2], user)
            with tarfile.open(t[2]) as tar:
                logging.info(f'Extract \'{t[1]}\' to: {t[2]}')
                obj_path = path.join(root_dir,t[0])
                tar.extractall(path=obj_path,members=change_target_dir(t[0],tar.getmembers()))
                #toggle_mockapi(comment=False)

    if args.gitcommit :

        folder = path.join(args.object_type,args.object_name)
        logging.info(f'Auto-commit of downloaded artifact: {folder}')
        run(['git','add',folder])
        run(['git','commit','-m','didownload'])

if __name__ == '__main__':
    main()