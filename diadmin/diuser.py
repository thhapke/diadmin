#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
# Using vctl
# URL: https://help.sap.com/viewer/0b99871a1d994d2ea89598fe59d16cf9/3.0.2/en-US/38f6d81551c44f5da0f10bd0249d67f1.html#loio38f6d81551c44f5da0f10bd0249d67f1


import csv
import sys
from pprint import pprint
import logging

import yaml
import argparse
import re
from os import path
import pandas as pd

from diadmin.vctl_cmds.login import di_login
from diadmin.vctl_cmds.user import create_user, assign_policies, deassign_policy, get_users, delete_user
from diadmin.vctl_cmds.policy import get_policy_list_assignments
from diadmin.utils.genpwds import gen_pwd
from diadmin.utils.utils import add_defaultsuffix, csvlist

MAX_DELETE = 3


# Create pwd for new user
# if password is None or ''
def create_password(users,password_length = 8) :
    for user in users :
        if not user['pwd'] :
            user['pwd'] = gen_pwd(password_length)

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



#### TESTS
def test_delete(connection, userlist, user) :
    ruser = {'user':'newuser','tenant':'pm-dev1'}

    userlist.append(ruser)
    create_user(connection,ruser)

    # from userlist
    delete_from_userlist(userlist,ruser)
    # from di
    delete_user(ruser)

def update_userlist(params, userlist) :
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
    
    

def generate_userlist(userlist,
                      filename = 'userlist.csv',
                      tenant = 'default',
                      format = '%firstname %name',
                      num=10,
                      user_prefix = 'user-',
                      pwd='Welcome01!',
                      pwd_length = 8,
                      role='STANDARD') :
    users = list()
    if userlist :
        with open(path.join('users',userlist),mode='r',newline='\n') as csvfile :
            if format == '%firstname %name':
                csvreader = csv.reader(csvfile,delimiter = ' ')
                for line in csvreader:
                    if line[0][0] == '#' :
                        continue
                    users.append({'tenant':tenant,'user':line[0].lower(),'name':' '.join(line),'pwd':gen_pwd(pwd_length),'role':role})
            elif format == '%name %firstname':
                csvreader = csv.reader(csvfile,delimiter = ' ')
                for line in csvreader:
                    if line[0][0] == '#' :
                        continue
                    users.append({'tenant':tenant,'user':line[-1].lower(),'name':' '.join(line),'pwd':gen_pwd(pwd_length),'role':role})
            else :
                raise ValueError(f'Unknown format: {format}')
    else:
        zf = 3 if num >= 99 else 2
        for i in range(1,num+1) :
            newuser = {'tenant':tenant,'user':user_prefix+str(i).zfill(zf),'name':'user','role':role}
            if pwd and pwd != 'RANDOM':
                newuser['pwd'] = pwd
            else :
                newuser['pwd'] = gen_pwd(pwd_length)
            users.append(newuser)

    # Test on duplicates
    # OPEN

    with open(path.join('users',filename),'w') as fp :
        for u in users :
            user_row = [u['tenant'],u['user'],u['name'],u['pwd'],u['role']]
            fp.write(','.join(user_row)+'\n')

def user_assignment_matrix(user_assignment,file) :

    users = set()
    roles = set()
    for r in user_assignment :
        users.add(r['user'])
        roles.add(r['id'])
    roles = dict(zip(roles,[None]*len(roles)))
    user_role = { u : dict(roles)  for u in users}
    for u in user_assignment :
        user_role[u['user']][u['id']] = 'x'
    df = pd.DataFrame(user_role)
    df.index.name = 'roles'
    df['sum'] = df.count(axis = 1)
    df = df.sort_values(['sum'],ascending=False)
    df = df.append(df.count().rename('sum'))
    df = df.sort_values(df.last_valid_index(), axis=1)
    df.drop(columns = ['sum'],inplace=True)
    df.drop('sum',inplace=True)
    df.to_csv(file)



############### MAIN #######################

def main() :

    logging.basicConfig(level=logging.INFO)

    description =  "User management.\nPre-requiste: vctl."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c','--config', help = 'Specifies yaml-config file',default='config.yaml')
    parser.add_argument('-p','--assign_policies', nargs='+', help = 'Assign policies to user in userlist')
    parser.add_argument('-l','--userlist', help = 'List of user to generate new userlist. If <nameless> userlist without names are generated.')
    parser.add_argument('-e','--deassign_policies', nargs='+', help = 'Deassign policies from user in userlist')
    parser.add_argument('-r','--role', help = 'User role with policies defined in config.',default='STANDARD')
    parser.add_argument('-g','--generate', help = 'Generate userlist',action='store_true')
    parser.add_argument('-o','--output', help = 'Output file of generated userlist',action='store_true')
    parser.add_argument('-a','--add', help = 'Add users of userlist',action='store_true')
    parser.add_argument('-d','--delete', help = 'Delete users of userlist',action='store_true')
    parser.add_argument('-w','--download', help = 'Download user to userlist',action='store_true')
    parser.add_argument('-s','--assignments', help = 'Download SAP Di user with assignements',action='store_true')

    args = parser.parse_args()

    if args.config:
        config_file = add_defaultsuffix(args.config,'yaml')
    with open(config_file) as yamls:
        params = yaml.safe_load(yamls)

    if args.generate :
        if args.userlist == 'nameless' :
            logging.info('Generate \'nameless\'-userlist')
            generate_userlist(userlist=None,
                              filename=params['USERLIST']['LIST'],
                              num=params['USERLIST']['NUMBER'],
                              pwd= params['USERLIST']['PASSWORD'],
                              pwd_length= params['USERLIST']['PWD_LENGTH'],
                              tenant=params['TENANT'],
                              user_prefix=params['USERLIST']['PREFIX'],
                              role=params['USERLIST']['DEFAULT_ROLE'])
        else :
            logging.info(f'Generate userlist from input userlist {args.userlist}')
            generate_userlist(userlist = args.userlist,
                              filename=params['USERLIST']['LIST'],
                              format = params['USERLIST']['FORMAT'],
                              pwd= params['USERLIST']['PASSWORD'],
                              pwd_length= params['USERLIST']['PWD_LENGTH'],
                              tenant=params['TENANT'])

    userlist = csvlist(path.join('users',params['USERLIST']['LIST']))

    if args.add :
        di_login(params)
        userlist.with_comments = False
        userlist.filter = ('status','TO_ADD')
        for u in userlist :
            if not u['password']:
                u['password'] = gen_pwd()
            u['status'] = 'EXISTS'
            create_user(u,'member')
            deassign_policy(u,'sap.dh.member')
            policies = params['USER_ROLE'][u['role']]
            assign_policies(u,params["USER_ROLE"][u["role"]])
            logging.info(f'Create user: {u["user"]} with role:{u["role"]} ({policies})')
        userlist.save()

    if args.delete :
        di_login(params)
        userlist.filter = ('status','TO_DELETE')
        logging.info(f"Delete user marked in user list with \'status\': \'TO_DELETE\'.")
        for i,u in enumerate(userlist) :
            if i >= MAX_DELETE:
                break
            delete_user(u)
            userlist.remove(u)

    if args.assign_policies :
        di_login(params)
        users = read_userlist(args.userlist)
        for u in users :
            logging.info(f'To user \'{u}\' assigned policies: {args.assign_policies}')
            assign_policies(u,args.assign_policies)

    if args.assignments:
        di_login(params)
        ur = get_policy_list_assignments()
        #with open(path.join('users','assignments.json'),'w') as fp:
        #    json.dump(ur,fp,indent=4)
        ur = user_assignment_matrix(ur,path.join('users',params['USERLISTS']['ASSIGNMENT_FILE']))

    if args.download:
        di_login(params)
        users = get_users()
        userlist.extend(users)
        userlist.set_default({'status':'EXISTS'})


if __name__ == '__main__':
    main()
    #generate_userlist('user_ws.csv',num=20)
