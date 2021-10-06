import logging
from os import path
from subprocess import check_output, run, CalledProcessError

def list_files(dir) :
    logging.info(f'List file in folder: {dir}')
    logging.debug(f"vctl vrep user ls {dir}")
    cmd = ['vctl','vrep','user','ls',dir]
    try :
        files_list = check_output(cmd).decode('utf-8').split('\n')
    except CalledProcessError as cp :
        cmd = ' '.join(cp.cmd)
        logging.error(f'Cmd: {cmd} ')
        return None
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

    for d in dirs :
        files.extend(list_files(d))

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
