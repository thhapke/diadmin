#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
# Using vctl
# URL: https://help.sap.com/viewer/0b99871a1d994d2ea89598fe59d16cf9/3.0.2/en-US/38f6d81551c44f5da0f10bd0249d67f1.html#loio38f6d81551c44f5da0f10bd0249d67f1


import csv
from pprint import pprint
from subprocess import check_output, run
import logging

import yaml

from diadmin.vctl_cmds.login import di_login
from genpwds import gen_pwd

# Read csv userlist
# format: username, password, name
def read_userlist(filename) :
    users = list()
    with open(filename,mode='r',newline='\n') as csvfile :
        csvreader = csv.reader(csvfile,delimiter = ',')
        for line in csvreader:
            if line[0][0] == '#' :
                continue
            users.append({'tenant':line[0],'user':line[1],'pwd':line[2],'name':line[3]})
    return users

# Create pwd for new user
# if password is None or ''
def create_password(users,password_length = 8) :
    for user in users :
        if not user['pwd'] :
            user['pwd'] = gen_pwd(params['PWD_LENGTH'])

# Save userlist
def save_userlist(filename,users) :
    with open(filename,mode='w',newline='\n') as csvfile :
        writer = csv.DictWriter(csvfile,users[0].keys(), delimiter=',')
        for u in users :
            writer.writerow(u)

def find_add_users(users, compare_users) :
    additional_user = []
    for u in users :
        found = False
        for uc in compare_users :
            if u['user'] == uc['user'] and u['tenant'] == uc['tenant'] :
                found = True
                break
        if not found :
            additional_user.append(u)

    return additional_user

def add_di_user(di_users,users) :
    for u in di_users :
        users.append({'tenant':u['tenant'],'user':u['user'],'pwd':'UNKNOWN','name':u['user']})
    return users

def get_user(userlist,user) :
    for u in userlist :
        if u['user'] == user['user'] and u['tenant'] == user['tenant']  :
            return u['user']

def delete_from_userlist(userlist,user) :
    for i,u in enumerate(userlist) :
        if u['user'] == user['user'] and u['tenant'] == user['tenant'] :
            logging.info(f"Delete user from user list: {user}")
            userlist.remove(u)

############### VCTL Calls #######################



# create user from userlist
def create_user(connection,user) :
    create_user = ['vctl','user','create',connection['TENANT'],user['user'],user['pwd'],'member']
    run(create_user)

# assign policy to user
def assign_policies(policies,user) :
    for p in policies :
        assign_policy = ['vctl','policy','assign',p,user['user']]
        run(assign_policy)

# deassign policy to user
def deassign_policies(policies,user) :
    for p in policies :
        deassign_policy = ['vctl','policy','deassign',p,user['user']]
        run(deassign_policy)

# get di user
def get_users() :
    userlist = ['vctl','user','list']
    di_user_list = check_output(userlist).decode('utf-8').split('\n')
    users =  [ u.split() for u in di_user_list][1:-1]
    return [{'tenant':u[0],'user':u[1]} for u in users]

def delete_user(user) :
    userdelete = ['vctl','user','delete',user['tenant'],user['user']]
    run(userdelete)

#### TESTS
def test_delete(connection, userlist, user) :
    ruser = {'user':'newuser','tenant':'pm-dev1'}

    userlist.append(ruser)
    create_user(connection,ruser)

    # from userlist
    delete_from_userlist(userlist,ruser)
    # from di
    delete_user(ruser)

def update_userlist(userlist) :
    userlist = read_userlist(params['USERLIST'])
    create_password(userlist) # creates only password for users without one

    # Read  di userlist
    di_users = get_users()

    # Compare user lists
    additional_di_user = find_add_users(di_users,userlist)
    add_di_user(additional_di_user,userlist)

    print("Additional di user: ")
    pprint(additional_di_user)
    
    add_userlist = find_add_users(userlist,di_users)
    
    print("Additional new user: ")
    pprint(add_userlist)
    
    # Add new user
    for u in add_userlist :
        create_user(params,u)
        assign_policies(params['POLICIES'],u)

    save_userlist(params['USERLIST'], userlist)
    
    



############### MAIN #######################

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    with open('../config.yaml') as yamls:
        params = yaml.safe_load(yamls)


    #### LOGIN
    di_login(params)
    
    di_user = get_users()
    pprint(di_user)
