import logging
import argparse
from urllib.parse import urljoin

import json
from os import path

import yaml
import csv
import pandas as pd

from diadmin.metadata_api.browse_connection import get_connection_datasets
from diadmin.utils.utils import add_defaultsuffix, mksubdir, get_system_id


def main():
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description = "Download factsheets from SAP Data Intelligence Connections."
    parser = argparse.ArgumentParser(description=description)
    help_config = 'Specifies config_demo.yaml file with the parameters: URL, TENANT, USER, PWD'
    parser.add_argument('connection_id', help="Connection ID")
    parser.add_argument('path', help="Path to folder/dataset")
    parser.add_argument('-v', '--verify', help='Verification of requests', action='store_true')
    parser.add_argument('-l', '--lineage', help='Adds lineage', action='store_true')
    parser.add_argument('-c', '--config', help = help_config, default='config.yamls')
    args = parser.parse_args()

    config_file = 'config.yaml'
    if args.config:
        config_file = add_defaultsuffix(args.config, 'yaml')
    with open(config_file) as yamls:
        params = yaml.safe_load(yamls)

    conn = {'url': urljoin(params['URL'], '/app/datahub-app-metadata/api/v1'),
            'auth': (params['TENANT']+'\\' + params['USER'],params['PWD'])}

    factsheets = list()
    datasets = get_connection_datasets(conn,
                                       connection_id=args.connection_id,
                                       container=args.path,
                                       factsheets = factsheets,
                                       lineage=args.lineage,
                                       verify=args.verify)

    print(json.dumps(factsheets,indent=4))


if __name__ == '__main__':
    main()
