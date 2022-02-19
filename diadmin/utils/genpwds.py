#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#


import string
import secrets

# set of chars from which to choose password chars
baseset = string.ascii_letters + string.digits
baseset = [i for i in baseset if not i in 'Il0O']
# special_chars = list(',.;:!?=+*#')
# baseset = baseset + special_chars

def gen_pwd(len_pwd=8) :
    '''
    Generate password with a given length with ascii excluding ambigiuos characters
    :param len_pwd: Passeword length (default 8)
    :return: password
    '''
    while True:
        # create passwords from baseset that containts at least one upper-, lowercase and digit
        password = ''.join(secrets.choice(baseset) for i in range(len_pwd))
        if (any(c.islower() for c in password)
                and any(c.isupper() for c in password)
                and sum(c.isdigit() for c in password ) >0 ) :
            break
    return password

def gen_user_pwd_list(num_user = 10, len_pwd = 8, prefix = 'user_') :
    '''
    Generates a generic user-password list with a given user prefix. Used for workshops
    :param num_user: Number of users (default 10)
    :param len_pwd: Lenght of password (default 8)
    :param prefix: User prefix (default user_
    :return: dictionary (dict[user]=pwd
    '''
    zfillnum = 3 if num_user  > 100 else 2

    user_pwd = dict()
    for i in range(0, num_user) :
        user = prefix + str(i).zfill(zfillnum)
        user_pwd[user] = gen_pwd(len_pwd=len_pwd)
    return user_pwd


if __name__ == '__main__':
    user_pwd = gen_user_pwd_list()
    print(user_pwd)
