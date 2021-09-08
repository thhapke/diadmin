

import string
import secrets

# set of chars from which to choose password chars
baseset = string.ascii_letters + string.digits
baseset = [i for i in baseset if not i in 'Il0O']
# special_chars = list(',.;:!?=+*#')
# baseset = baseset + special_chars

def gen_pwd(len_pwd=8) :
    while True:
        # create passwords from baseset that containts at least one upper-, lowercase and digit
        password = ''.join(secrets.choice(baseset) for i in range(len_pwd))
        if (any(c.islower() for c in password)
                and any(c.isupper() for c in password)
                and sum(c.isdigit() for c in password ) >0 ) :
            break
    return password

def gen_user_pwd_list(num_user = 10, len_pwd = 8, prefix = 'user_') :
    zfillnum = 3 if num_user  > 100 else 2

    user_pwd = dict()
    for i in range(0, num_user) :
        user = prefix + str(i).zfill(zfillnum)
        user_pwd[user] = gen_pwd(len_pwd=len_pwd)
    return user_pwd


if __name__ == '__main__':
    user_pwd = gen_user_pwd_list()
    print(user_pwd)
