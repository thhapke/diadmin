
import logging
import argparse
from os import listdir, path, remove
from pathlib import Path
import re
import zipfile
from subprocess import run

def _copy(self, target):
    import shutil
    assert self.is_file()
    shutil.copy(str(self), str(target))  # str() only there for Python < (3, 6)

Path.copy = _copy

def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  "Open logfile."
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('-d','--directory', help='Directory of downloaded log-files',default=".")
    parser.add_argument('-r','--remove', help='Remove downloaded file',action='store_true')
    parser.add_argument('-I','--removeinfo', help='Remove INFO lines',action='store_true')
    parser.add_argument('-P','--removeprefix', help='Remove prefix string from each line',action='store_true')
    parser.add_argument('-i','--keepinfo', help='Keep INFO lines with substring.')
    parser.add_argument('-e','--editor', help='Editor for open the log',default='atom')
    args = parser.parse_args()

    log_dir = args.directory
    logging.info(f"Search for logs in directory: {args.directory}")
    zip_files = [path.join(log_dir, f) for f in listdir(log_dir) if path.isfile(path.join(log_dir, f)) and re.match('vflow-diagnostic.+\.zip',f)]

    if len(zip_files) == 0 :
        logging.warning(f'No zip-files in directory: {log_dir}')

    # unzip logs
    zip_dirs = list()
    for f in zip_files:
        with zipfile.ZipFile(f, 'r') as zip_ref:
            logging.info(f'Unzip file: {f}')
            zip_dir = path.basename(f)[:-4]
            zip_ref.extractall(path.join(log_dir,zip_dir))
            zip_dirs.append(zip_dir)

    # open operator log
    log_files = list()
    for logd in zip_dirs :
        for pf in listdir(path.join(log_dir,logd)) :
            if (not path.isdir(path.join(log_dir,logd,pf))) or (pf in ['api-pods','build-logs']):
                continue
            for plf in listdir(path.join(log_dir,logd,pf,'default','vflow')) :
                logtxt = Path(log_dir) / logd / pf/ 'default' / 'vflow' / plf
                cpylogtext = Path(log_dir) / plf
                logging.info(f"Copy log text \'{logtxt}\' to \'{cpylogtext}\'")
                logtxt.copy(cpylogtext )
                log_files.append(cpylogtext)

    for zf in zip_files:
        logging.info(f'Remove zip-file: {zf}')
        remove(zf)

    if args.remove :
        for zd in zip_dirs :
            zip_dir = Path(log_dir) / zd
            logging.info(f'Remove zip-dir: {zip_dir}')
            rm_tree(zip_dir)

    if args.keepinfo :
        keep_string = '|'+args.keepinfo

    prefix_pattern = re.compile('.+\|\+\d{4}(\|.+)')

    for lt in log_files :
        if args.removeinfo or args.removeprefix:
            logging.info(f"Remove INFO Lines from {lt}")
            log_text = ""
            with open(lt,"r") as fp :
                line = fp.readline()
                while line :
                    if args.removeprefix and prefix_pattern.match(line) :
                        line = prefix_pattern.match(line).group(1)+'\n'
                    if (args.removeinfo and not '|INFO' in line) or (args.keepinfo and keep_string in line):
                        log_text += line
                    line = fp.readline()
                fp.close()
            with open(lt,'w') as fp:
                fp.write(log_text)
                fp.close()

        logging.info(f"Open log file \'{lt}\' with \'{args.editor}\'")
        run([args.editor,lt])

def rm_tree(pth: Path):
    for child in pth.iterdir():
        if child.is_file():
            child.unlink()
        else:
            rm_tree(child)
    pth.rmdir()


if __name__ == '__main__':
    main()