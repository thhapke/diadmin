#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
import os
from os import path,makedirs,getcwd, walk, listdir,mkdir
import logging
import argparse
import tarfile
import re
import json
from subprocess import run

import yaml

from diadmin.utils.utils import add_defaultsuffix, toggle_mockapi
from diadmin.vctl_cmds.login import di_login
from diadmin.vctl_cmds.vrep import import_object, solution_to_repo, import_menue_panel,VFLOW_PATHS
from diadmin.utils.utils import read_userlist, get_operator_generation

# global variable
root_dir = '.'
LOCAL_TEST = False
OBJECT_TYPES = ['project','operators','operators_gen2','graphs','dockerfiles','vtypes','general']

def exclude_files(tarinfo) :
    basename = path.basename(tarinfo.name)
    if re.match('\..+',basename) or re.match('.+\.tgz',basename) or basename == '__pycache__' :
        return None
    else:
        return tarinfo

def get_sources(object_type, source) :
    if object_type == 'all' or object_type == '*' :
        sources =  dict(zip(OBJECT_TYPES,OBJECT_TYPES))
    elif source == '.' or source == '*' :
        sources = {object_type: [object_type]}
    elif object_type in OBJECT_TYPES and not object_type == 'project':
        if not object_type == 'general':
            source = source.replace('.',os.sep)
        sources = {object_type: [source]}
    elif object_type == 'project':
        project_file = add_defaultsuffix(source,'yaml')
        with open(path.join('projects',project_file)) as yamls:
            project = yaml.safe_load(yamls)
        sources = dict()
        for object_type, object_list in project.items() :
            if not object_type in OBJECT_TYPES:
                raise ValueError(f"Unknown object type in project file {source}: {object_type}")
            if object_list == None :
                continue
            sources[object_type] = list()
            for object_name in object_list :
                if not object_type == 'general':
                    object_name = object_name.replace('.',os.sep)
                sources[object_type].append(object_name)
    else :
        raise ValueError(f"Unknown object type: {object_type}")
    return sources

def make_tarfiles(sources) :
    tarfiles = dict()
    for object_type,object_names in sources.items() :
        tar_filename = path.join(root_dir,object_type+'.tgz')
        tarfiles[object_type] = tar_filename
        with tarfile.open(tar_filename, "w:gz") as tar:
            zip_path = path.join(root_dir,object_type)
            for object_name in object_names :
                obj_path = path.join(zip_path,object_name)
                if object_type == 'operators' or object_type == 'operators_gen2':
                    toggle_mockapi(obj_path,comment = True)
                for d in listdir(obj_path) :
                    sd = path.join(obj_path,d)
                    tar.add(sd,arcname=path.relpath(sd,zip_path),filter=exclude_files)
                if object_type == 'operators' or object_type == 'operators_gen2' :
                    toggle_mockapi(obj_path,comment = False)
    return tarfiles

def main() :
    global root_dir
    logging.basicConfig(level=logging.INFO,format='%(levelname)s:%(message)s')

    #
    # command line args
    #
    achoices = OBJECT_TYPES + ['all','*']
    description =  "Uploads operators, graphs and  dockerfiles to SAP Data Intelligence.\nPre-requiste: vctl."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-i','--init', help = 'Creates a config.yaml and the necessary folders. Additionally you need '
                                              'to add \'* *\' as dummy positional arguments',action='store_true')
    parser.add_argument('-c','--config', help = 'Specifies yaml-config file',default='config_demo.yaml')
    parser.add_argument('-r','--conflict', help = 'Conflict handling flag of \'vctl vrep import\'')
    parser.add_argument('object_type', help='Type of artifacts.',choices=achoices)
    parser.add_argument('object_name', help='Artifact file(tgz) or directory')
    parser.add_argument('-n', '--solution', help='Solution name if uploaded artificats should be exported to solution repository as well.')
    parser.add_argument('-s', '--description', help='Description string for solution.')
    parser.add_argument('-v', '--version', help='Version of solution. Necessary if exported to solution repository.',default='0.0.1')
    parser.add_argument('-u', '--user', help='SAP Data Intelligence user if different from login-user. '
                                             'For value "userlist" artifact uploaded to all user in userlist.'
                                             ' Not applicable for solutions-upload')
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
        config_file = add_defaultsuffix(args.config,'yaml')
    with open(config_file) as yamls:
        params = yaml.safe_load(yamls)

    if 'ROOT_DIR' in params:
        root_dir = params['ROOT_DIR']
    ret = 0
    if not LOCAL_TEST :
        ret = di_login(params)
    if not ret == 0 :
        return ret

    user = params['USER']
    if  args.user :
        user = args.user

    conflict = None
    if args.conflict :
        conflict = args.conflict


    #if args.object_name_type == 'ui' :
        #args.object_name ='vsolution_vflow_pa_settings.json'

    if (re.match('.+\.tgz$',args.object_name) or re.match('.+\.tar.gz$',args.object_name)):
        if not LOCAL_TEST :
            if user == 'userlist' :
                userlist = read_userlist(params['USERLIST']['LIST'])
                for u in userlist :
                    import_object(args.object_type,args.object_name,u['user'],conflict)
            else :
                import_object(args.object_type,args.object_name,user,conflict)
    else :
        sources = get_sources(args.object_type,args.object_name)
        tarfiles = make_tarfiles(sources)
        if not LOCAL_TEST :
            if user == 'userlist' :
                userlist = read_userlist(params['USERLIST']['LIST'])
                for u in userlist :
                    for st,fn in tarfiles.items() :
                        import_object(st,fn,u['user'],conflict)
            else :
                for st,fn in tarfiles.items() :
                    import_object(st,fn,user,conflict)

    if args.solution:
        if re.match('.+\.tgz$',args.object_name):
            basename =  re.match('(.+)\.tgz$',args.object_name).group(0)
        elif re.match('.+\.tar.gz$',args.object_name) :
            basename = re.match('(.+)\.tar.gz$',args.object_name).group(0)
        elif args.object_name == '*' or args.object_name == '.' :
            basename = args.object_name_type
        else :
            basename = args.object_name
        basename = path.basename(basename)
        source = path.join(VFLOW_PATHS[args.object_type],basename)
        solution_to_repo(source, args.solution, args.version, args.description)

    if args.gitcommit :
        folder = path.join(args.object_name_type,args.object_name)
        logging.info(f'Auto-commit for uploaded artifact: {folder}')
        run(['git','add',folder])
        run(['git','commit','-m','diupload'])


if __name__ == '__main__':
    main()