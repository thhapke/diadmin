
import logging
import argparse
from os import listdir, path, remove
import re
import zipfile
from subprocess import run

def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  "Open logfile."
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('-d','--directory', help='Directory of downloaded log-files',default=".")
    parser.add_argument('-r','--remove', help='Remove downloaded file',action='store_true')
    parser.add_argument('-e','--editor', help='Editor for open the log',default='atom')
    args = parser.parse_args()

    log_dir = args.directory
    logging.info(f"Search for logs in directory: {args.directory}")
    zip_files = [path.join(log_dir, f) for f in listdir(log_dir) if path.isfile(path.join(log_dir, f)) and re.match('vflow-diagnostic.+\.zip',f)]

    # unzip logs
    log_dirs = list()
    for f in zip_files:
        with zipfile.ZipFile(f, 'r') as zip_ref:
            logging.info(f'Unzip file: {f}')
            zip_dir = path.basename(f)[:-4]
            zip_ref.extractall(path.join(log_dir,zip_dir))
            log_dirs.append(zip_dir)

    # open operator log
    log_files = list()
    for logd in log_dirs :
        for pf in listdir(path.join(log_dir,logd)) :
            if (not path.isdir(path.join(log_dir,logd,pf))) or (pf in ['api-pods','build-logs']):
                continue
            for plf in listdir(path.join(log_dir,logd,pf,'default','vflow')) :
                logfile = path.join(log_dir,logd,pf,'default','vflow',plf)
                run([args.editor,logfile])
                log_files.append(logfile)

    if args.remove:
        for zf in zip_files:
            logging.info(f'Remove zip-file: {zf}')
            remove(zf)

if __name__ == '__main__':
    main()