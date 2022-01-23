#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#


import logging
import argparse
import json
from os import path
from urllib.parse import urljoin

import yaml

from diadmin.utils.utils import add_defaultsuffix
from diadmin.metadata_api.container import get_connections

def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  "Connections from Connections Management (only download implemented)"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c','--config', help = 'Specifies config_demo.yaml file with the parameters: URL, TENANT, USER, PWD' ,
                        default='config.yamls')
    #parser.add_argument('-i','--importcon', help = "Import connection")
    parser.add_argument('-d','--download', help = "Downloads all connections",action='store_true')
    args = parser.parse_args()

    config_file = 'config.yaml'
    if args.config:
        config_file = add_defaultsuffix(args.config,'yaml')
    with open(config_file) as yamls:
        params = yaml.safe_load(yamls)

    conn = {'url': urljoin(params['URL'] , '/app/datahub-app-metadata/api/v1'),
            'auth':(params['TENANT']+'\\'+ params['USER'],params['PWD'])}

    #resturl = params['URL'] + '/app/datahub-app-connection/connections' OLD Option

    if args.download :
        cts = get_connections(conn,filter_type='',filter_tags='')

        with open(path.join('connections','connections.json'),'w') as fp:
            json.dump(cts,fp,indent=4)

if __name__ == '__main__':
    main()