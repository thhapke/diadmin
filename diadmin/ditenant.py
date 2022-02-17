#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

import logging
import argparse
import re

import yaml


from diadmin.vctl_cmds.login import di_login
from diadmin.vctl_cmds.tenant import get_info,get_configuration
from diadmin.utils.utils import add_defaultsuffix



def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  "Get tenant info.\nPre-requiste: vctl."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('command', help = 'Tenant command [get,configuration,list]',default='get')
    parser.add_argument('-t','--tenant', help = 'tenant name',default='default')
    parser.add_argument('-c','--config', help = 'Specifies yaml-config file',default='config.yaml')
    parser.add_argument('-o','--format', help = 'Output format',default='json')
    args = parser.parse_args()


    config_file = add_defaultsuffix(args.config,'yaml')
    with open(config_file) as yamls:
        params = yaml.safe_load(yamls)

    di_login(params)
    if args.command == 'get':
        info_dict, info_str = get_info(args.tenant,args.format)
    elif args.command == 'configuration':
        info_dict, info_str = get_configuration()
    print(info_str)


if __name__ == '__main__':
    main()