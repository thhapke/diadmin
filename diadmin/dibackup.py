#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

import logging
import argparse
import re
from os import path,mkdir,rmdir

import yaml

from diadmin.utils.utils import add_defaultsuffix
from diadmin.vctl_cmds.login import di_login
from diadmin.vctl_cmds.user import get_users
from diadmin.vctl_cmds.vrep import export_artifact

VFLOW_PATHS = {'operators':'files/vflow/subengines/com/sap/python36/operators/',
               'graphs':'files/vflow/graphs/',
               'dockerfiles':'files/vflow/dockerfiles/'}

ARTIFACTS = ['operators','graphs','dockerfiles']

def mksubdir(parentdir,dir) :
    newdir = path.join(parentdir,dir)
    if not path.isdir(newdir) :
        logging.info(f"Make directory: {newdir}")
        mkdir(newdir)
    return newdir

def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    achoices = ['operators','graphs','dockerfiles','menu','all','*','solution']
    description =  "Backup development artifacts of all users.\nPre-requiste: vctl."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c','--config', help = 'Specifies yaml-config file',default='config_demo.yaml')
    args = parser.parse_args()

    if args.config:
        config_file = add_defaultsuffix(args.config,'yaml')
    with open(config_file) as yamls:
        params = yaml.safe_load(yamls)

    disystem = re.match('.+vsystem\.ingress\.([a-zA-Z0-9-_]+)\.([a-zA-Z0-9-_]+).+',params['URL'])
    disystem = disystem.group(1) + '.' + disystem.group(1) + '-'+ params['TENANT']

    backupsdir = mksubdir('.','backups')
    systemdir = mksubdir(backupsdir,disystem)

    ret = di_login(params)
    if not ret == 0 :
        return ret

    # get all user
    users = get_users()

    #users = [users[0]]
    for u in users :
        systemdiruser = mksubdir(systemdir,u['user'])
        logging.info(f"Download artifacts of user: {u['user']}")
        count_exported = 0
        for a in ARTIFACTS :
            file = path.join(systemdiruser,a+'.tgz')
            count_exported += export_artifact(a,a,file,u['user'])
        if count_exported == 0 :
            logging.info(f'No artifacts to backup.')
            rmdir(systemdiruser)


if __name__ == '__main__':
    main()