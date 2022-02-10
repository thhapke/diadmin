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
from diadmin.vctl_cmds.tenant import get_info



def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  "Get tenant info.\nPre-requiste: vctl."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c','--config', help = 'Specifies yaml-config file')
    parser.add_argument('-o','--format', help = 'Output format',default='json')
    args = parser.parse_args()

    if args.config:
        config_file = args.config
        if not re.match('.+\.yaml',config_file) :
            config_file += '.yaml'
    with open(config_file) as yamls:
        params = yaml.safe_load(yamls)

    di_login(params)
    info_dict, info_str = get_info(params['TENANT'],args.format)
    print(info_str)


if __name__ == '__main__':
    main()