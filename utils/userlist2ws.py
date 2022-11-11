from os import path
import argparse
import csv

from icecream import ic


description = "Converts userlist to workshop user list."
parser = argparse.ArgumentParser(description=description)
parser.add_argument('userlist', help='Userlist to convert.')

args = parser.parse_args()

userlist = list()
with open(args.userlist, mode='r', newline='\n') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    header = next(csvreader)
    for line in csvreader:
        if line[0][0] == '#':
            continue
        userlist.append([line[1], line[3]])

ic(userlist)
output = args.userlist[:-4] + '_ws.csv'

with open(output, mode='w', newline='\n') as csvfile:
    for user in userlist:
        csvfile.write(','.join(user) + '\n')

