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
from diadmin.vctl_cmds.vrep import export_object, VFLOW_PATHS

OBJECT_TYPES = ['operators','graphs','dockerfiles','vtypes','operators_gen2']

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
    description =  "Backup development objects of all users.\nPre-requiste: vctl."
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('cluster', help = 'URL of cluster')
    parser.add_argument('tenant', help = 'tenant name')
    parser.add_argument('-u','--user', help = 'Admin user')
    parser.add_argument('-p','--password', help = 'Password')
    args = parser.parse_args()

    disystem = re.match('.+vsystem\.ingress\.([a-zA-Z0-9-_]+)\.([a-zA-Z0-9-_]+).+',args.cluster)
    disystem = disystem.group(1) + '.' + disystem.group(2) + '-'+ args.tenant

    backupsdir = mksubdir('.','backups')
    systemdir = mksubdir(backupsdir,disystem)

    ret = di_login({'URL':args.cluster,'TENANT':args.tenant,'USER':args.user,'PWD':args.password})
    if not ret == 0 :
        return ret

    # get all user
    users = get_users()

    #users = [users[0]]
    for u in users :
        systemdiruser = mksubdir(systemdir,u['user'])
        logging.info(f"Download objects of user: {u['user']}")
        count_exported = 0
        for object_type in OBJECT_TYPES :
            file = path.join(systemdiruser,object_type+'.tgz')
            count_exported += export_object(object_type, None, file, u['user'])
        if count_exported == 0 :
            logging.info(f'No development objects to backup.')
            rmdir(systemdiruser)


if __name__ == '__main__':
    main()