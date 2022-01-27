#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
import json
from os import path
import logging
import yaml
import argparse
from pprint import pprint
from vctl_cmds.login import di_login
from vctl_cmds.policy import get_policy_resources,create_policy

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  """"Copies policy with resources to new policy. Pre-requiste: vctl.
    Download vctl: https://launchpad.support.sap.com/#/softwarecenter/template/products/%20_APP=00200682500000001943&_EVENT=DISPHIER&HEADER=Y&FUNCTIONBAR=N&EVENT=TREE&NE=NAVIGATE&ENR=73554900100800002981&V=INST&TA=ACTUAL&PAGE=SEARCH/DATA%20INTELLIGENCE-SYS%20MGMT%20CLI"
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c','--config', help = 'Specifies yaml-config file',default='config.yamls')
    parser.add_argument('-p', '--policy', help='Source policy')
    parser.add_argument('-n', '--newpolicy', help='New policy id' )
    args = parser.parse_args()


    with open(args.config) as yamls:
        params = yaml.safe_load(yamls)

    di_login(params)

    pcy = get_policy_resources(args.policy)
    pcy['id'] = args.newpolicy
    pcy['description'] = 'COPY: ' + pcy['description']
    pcy['predelivered'] = False
    #pprint(pcy)

    filename = path.join(params['OUTPUT_PATH'],pcy['id'] + '.json')
    logging.info(f"Write new policy to: {filename}")
    with open(filename, 'w') as json_file:
        json.dump(pcy,json_file,indent=4)

    create_policy(filename)