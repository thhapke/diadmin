#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

import logging
import argparse

import requests
import yaml

from diadmin.utils.utils import add_defaultsuffix

def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  "Build a docker image for a user."
    parser = argparse.ArgumentParser(description=description)
    help_config = 'Specifies config_demo.yaml file with the parameters: URL, TENANT, USER, PWD'
    parser.add_argument('dockerpath', help = "Dockerfile path")
    parser.add_argument('-c','--config', help = help_config,default='config.yamls')
    args = parser.parse_args()

    config_file = 'config.yaml'
    if args.config:
        config_file = add_defaultsuffix(args.config,'yaml')

    with open(config_file) as yamls:
        params = yaml.safe_load(yamls)


    auth = (params['TENANT']+'\\'+params['USER'], params['PWD'])
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    resturl = params['URL'] + '/app/pipeline-modeler/service/v1/dockerenv/deploy' + '/' + args.dockerpath

    logging.info(f'HTTP request for building docker image: {resturl}')
    resp = requests.put(resturl, auth=auth, headers=headers,verify = True)

    if resp.status_code == 202 :
        logging.info(f'Successful request: {resp.status_code} - {resp.text}')
    else :
        logging.error(f'HTTP status: {resp.status_code} - {resp.text}')

if __name__ == '__main__':
    main()