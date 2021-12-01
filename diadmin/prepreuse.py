#!/usr/bin/python

import argparse
import os
import logging
import re
from shutil import copyfile
from pprint import pprint

# GLOBAL Variables
license_xml = ''
license_hash = ''
license_blank  = ''


def insert_license(file,license_param) :
    extension = os.path.splitext(file)[1]
    if not extension in license_param.keys() :
        logging.warning('Untreated file: {}'.format(file))
        return -1
    with open(file, "r") as txtfile:
        content = txtfile.readlines()
        if not any('SPDX-FileCopyrightText' in line for line in content):
            logging.info(f'Adding license info to file:{file}')
            content.insert(license_param[extension]['offset'], license_param[extension]['string'])
        else:
            logging.info('Is already under license {}'.format(file))
            return 0
        txtfile.close()

    logging.info('Adding license {} to {}'.format(license, file))
    with open(file, "w") as txtfile:
        txtfile.writelines(content)
    return 1

def add_license_file(file,license_str) :
    with open(file, "w") as txtfile:
        txtfile.writelines(license_str)
    txtfile.close()

def simple_parse_gitignore(file,ignores) :
    with open(file) as ignore_file:
        for line in ignore_file :
            line = line.rstrip('\n').strip()
            if line == '' or line[0] == '#':
                continue
            if re.search('(.+)\/\*?$',line) :
                ignores['dirs'].append(re.search('(.+)\/\*?$',line).group(1))
            elif re.search('\*\.(.+)$',line) :
                ignores['suffices'].append(re.search('\*\.(.+)$',line).group(1))
            elif re.search('(.+)\/\*$',line) :
                ignores['pending'].append(line)
            else :
                ignores['files'].append(line)

def files_to_license(root_path,ignores) :
    logging.info("Read .gitignore")
    lic_files = list()
    for path, subdirs, files in os.walk(root_path):
        subdirs[:] = [ d for d in subdirs if d not in ignores['dirs'] ]
        for f in files :
            bname,ext =os.path.splitext(f)
            rp = os.path.relpath(root_path,path)
            if not os.path.relpath(path,root_path) in ignores['dirs']  and \
                not ext in ignores['suffices'] and not f in ignores['files']  :
                lic_files.append(os.path.join(path,f))

    return lic_files

def add_licenses(files,add_license_ext,license_param) :
    logging.info("Add license information")
    for f in files :
        ext = os.path.splitext(f)[1]
        if ext in add_license_ext :
            # add licence file
            dir = os.path.dirname(f)
            f_license = os.path.join(dir,f + '.license')
            if not f_license in files :
                add_license_file(f_license,license_blank)
                logging.info(f'Adding license file:{f_license}')
            else :
                logging.info('License file exists: {}'.format(f_license))
        else:
            insert_license(f,license_param)


def main() :

    global license_xml, license_hash, license_blank

    logging.basicConfig(level=logging.INFO)

    ## Command line OPTIONS
    parser = argparse.ArgumentParser(description='Make files in this directory resuseable.')
    parser.add_argument('-y','--year', help='Year of Publication', default = '2021' )
    parser.add_argument('-n','--name', help='Owner\'s name',default='Thorsten Hapke')
    parser.add_argument('-e','--email', help='Owner\'s email', default = 'thorsten.hapke@sap.com')
    parser.add_argument('-l','--license', help='License to be used', default='Apache-2.0')
    argument = parser.add_argument('-r', '--root', help='root path', default='./')
    args = parser.parse_args()

    year = args.year
    name = args.name
    email = args.email
    license = args.license

    if args.root :
        root_path = args.root
    else :
        root_path = os.getcwd()

    if not os.path.isdir(root_path) :
        raise NotADirectoryError

    root_path = os.path.abspath(root_path)
    logging.info("Project path: {}".format(root_path))

    # check for LICENSES folder
    if os.path.isdir(os.path.join(root_path,'LICENSES')) :
        logging.info('LICENSE-folder exists')
    else :
        logging.info('LICENSE-folder created')
        license_folder = os.path.join(root_path,'LICENSES')
        os.mkdir(license_folder)
        src_license_folder = os.path.join(os.path.dirname(root_path),'LICENSES')
        copyfile(os.path.join(src_license_folder,license+'.txt'),os.path.join(license_folder,license + '.txt'))


    ignores = {'dirs':['.git'],'files':['__init__.py'],'suffices':[],'pending': []}
    simple_parse_gitignore(os.path.join(root_path,'.gitignore'),ignores)
    pprint(ignores['dirs'])

    license_xml = '<!--\n  SPDX-FileCopyrightText: {year} {name} <{email}>\n\n  SPDX-License-Identifier: {license}\n-->\n'.format(year=year,name = name,email=email,license=license)
    license_hash = '#\n#  SPDX-FileCopyrightText: {year} {name} <{email}>\n#\n#  SPDX-License-Identifier: {license}\n#\n'.format(year=year,name = name,email=email,license=license)
    license_blank = 'SPDX-FileCopyrightText: {year} {name} <{email}>\n\nSPDX-License-Identifier: {license}\n'.format(year=year,name = name,email=email,license=license)

    stars = '***********************************************************************'
    logging.info('License Information:\n{}\n{}{}'.format(stars,license_blank,stars))

    files_add_lfile = ['.png','.json','.pdf','.zip']
    license_param = {'.xml':{'string':license_xml,'offset':1},
                     'svg':{'string':license_xml,'offset':1},
                     '.py':{'string':license_hash,'offset':0},
                     '.md':{'string':license_xml,'offset':0}}

    files = files_to_license(root_path,ignores)
    pprint(files)
    add_licenses(files,files_add_lfile,license_param)


if __name__ == '__main__':
    main()