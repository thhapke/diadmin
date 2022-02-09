#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
# Using vctl
# URL: https://help.sap.com/viewer/0b99871a1d994d2ea89598fe59d16cf9/3.0.2/en-US/38f6d81551c44f5da0f10bd0249d67f1.html#loio38f6d81551c44f5da0f10bd0249d67f1


from subprocess import check_output, run
import logging

import yaml

from diadmin.vctl_cmds.login import di_login
from diadmin.utils.genpwds import gen_pwd


# create user from userlist
def create_user(user,role) :
    create_user = ['vctl','user','create',user['tenant'],user['user'],user['password'],role]
    run(create_user)

# assign policy to user
def assign_policies(user,policies) :
    for p in policies :
        assign_policy = ['vctl','policy','assign',p,user['user']]
        run(assign_policy)


# deassign policy to user
def deassign_policies(policies,user) :
    for p in policies :
        deassign_policy = ['vctl','policy','deassign',p,user['user']]
        run(deassign_policy)

def deassign_policy(user,policy) :
    deassign_policy = ['vctl','policy','deassign',policy,user['user']]
    run(deassign_policy)

# get di user
def get_users() :
    logging.info('Get user list')
    userlist = ['vctl','user','list']
    di_user_list = check_output(userlist).decode('utf-8').split('\n')
    users =  [ u.split() for u in di_user_list][1:-1]
    return [{'tenant':u[0],'user':u[1]} for u in users]

def delete_user(user) :
    logging.info(f"Delete user: {user}")
    userdelete = ['vctl','user','delete',user['tenant'],user['user']]
    run(userdelete)

