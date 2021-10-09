import logging
from os import path
from subprocess import check_output, run, CalledProcessError

VFLOW_PATHS = {'bundle':'/',
               'operators':'/files/vflow/subengines/com/sap/python36/operators/',
               'pipelines':'/files/vflow/graphs/',
               'dockerfiles':'/files/vflow/dockerfiles/'}

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

def import_artifact(artifact_type,file,user,flags=None) :
    if flags :
        cmd = ['vctl','vrep','user','import',file,VFLOW_PATHS[artifact_type],'-u',user,'-r',flags]
        logging.info(f'Import {artifact_type[:-1]}: {file} to user: {user} ({" ".join(cmd)})')
        run(cmd)
    else :
        cmd = ['vctl','vrep','user','import',file,VFLOW_PATHS[artifact_type],'-u',user]
        logging.info(f'Import {artifact_type[:-1]}: {file} to user: {user} ({" ".join(cmd)})')
        run(cmd)