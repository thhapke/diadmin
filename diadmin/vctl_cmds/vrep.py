#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
import logging
import subprocess
from os import path
from subprocess import check_output, run, CalledProcessError

VFLOW_PATHS = {'operators':'files/vflow/subengines/com/sap/python36/operators/',
               'operators_gen2':'files/vflow/subengines/com/sap/python3/operators/',
               'graphs':'files/vflow/graphs',
               'vtypes':'files/vflow/vtypes/',
               'dockerfiles':'files/vflow/dockerfiles/'}

def get_dir_files(dir) :
    logging.info(f'List file in folder: {dir}')
    logging.debug(f"vctl vrep user ls {dir}")
    cmd = ['vctl','vrep','user','ls',dir]
    try :
        files_list = check_output(cmd).decode('utf-8').split('\n')
    except CalledProcessError as cp :
        cmd = ' '.join(cp.cmd)
        logging.error(f'Cmd: {cmd} ')
        return None, None
    dirs = list()
    files = list()
    for f in files_list :
        if len(f) == 0 :
            continue
        if not '.' in f and not f=='Dockerfile' :
            dirs.append(path.join(dir,f))
            continue
        files.append(path.join(dir,f))
    logging.debug(f"Files: {files}")
    logging.debug(f"Directories: {dirs}")
    return files, dirs

def mkdir_p(root,dir) :
    files, dirs = get_dir_files(root)
    for d in dirs:
        if dir == path.basename(d) :
            return True
    else :
        newdir = path.join(root,dir)
        run(['vctl','vrep','user','mkdir',newdir])

def get_all_files(dir) :

    files, dirs = get_dir_files(dir)
    for d in dirs :
        files.extend(get_all_files(d))

    return files

def read_file(file) :
    logging.debug(f'Read file: {file}')
    cmd = ['vctl','vrep','user','cat',file]
    try :
        content = check_output(cmd).decode('utf-8')
    except CalledProcessError as cp :
        cmd = ' '.join(cp.cmd)
        logging.error(f'Cmd: {cmd} ')
        return None
    return content

def upload_file(source,target) :
    logging.info(f"Upload file: {source} -> {target}")
    run(['vctl','vrep','user','put',source,target])

def import_object(artifact_type, file, user, flags=None) :

    if flags :
        cmd = ['vctl','vrep','user','import',file,VFLOW_PATHS[artifact_type],'-u',user,'-r',flags]
        logging.info(f'Import {artifact_type[:-1]}: {file} to user: {user} ({" ".join(cmd)})')
        run(cmd)
    else :
        cmd = ['vctl','vrep','user','import',file,VFLOW_PATHS[artifact_type],'-u',user]
        logging.info(f'Import {artifact_type[:-1]}: {file} to user: {user} ({" ".join(cmd)})')
        run(cmd)

def export_object(object_type, object_name, file, user) :
    source = VFLOW_PATHS[object_type]
    if object_name :
        source = path.join(VFLOW_PATHS[object_type], object_name)
    cmd = ['vctl','vrep','user','export',file,source,'-u',user]
    logging.info(f'Export \'{source}\' of user \'{user}\' to file: {file} ({" ".join(cmd)})')
    try :
        check_output(cmd)
        return 1
    except subprocess.CalledProcessError as e :
        return 0

def export_menue_panel(file,user) :
    source =''
    cmd = ['vctl','vrep','user','export',file,source,'-u',user]
    logging.info(f'Export \'{source}\' of user \'{user}\' to file: {file} ({" ".join(cmd)})')
    run(cmd)

def import_menue_panel(file,user) :

    cmd = ['vctl','vrep','user','import',file,VFLOW_PATHS['menue_panel'],'-u',user]
    logging.info(f'Import \menue_panel\': {file} to user: {user} ({" ".join(cmd)})')
    run(cmd)

def solution_from_repo(solution_name, version) :
    run(['vctl','vrep','user','import-solution',solution_name,version])

def solution_to_repo(source, solution_name, version, description) :
    if description :
        cmd = ['vctl','vrep','user','export-solution',solution_name,version,source,'-s',description]
    else :
        cmd = ['vctl','vrep','user','export-solution',solution_name,version,source]
    logging.info(f'Export vrep  \'{source}\'  to solution: {solution_name}-{version} ({" ".join(cmd)})')
    run(cmd)