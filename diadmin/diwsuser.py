#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
import logging
import argparse
import csv
import os


############### MAIN #######################

def main() :

    logging.basicConfig(level=logging.INFO)

    description =  "Creates workshop user list out of userlist."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('userlist', help = 'userlist that is converted')
    args = parser.parse_args()

    with open(args.userlist,mode='r',newline='\n') as csvfile :
        csvreader = csv.reader(csvfile,delimiter = ',')
        users_str = ''
        for line in csvreader:
            if line[0][0] == '#' :
                continue
            users_str += line[1]+','+line[3] + '\n'

    file_path = os.path.dirname(os.path.realpath(args.userlist))
    new_userfile = os.path.join(file_path,'ws_'+os.path.basename(args.userlist))

    logging.info(f'New workshop userfile created: {new_userfile}')

    with open(new_userfile,mode='w') as nf :
        nf.write(users_str)


if __name__ == '__main__':
    main()

