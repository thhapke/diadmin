

import logging
import argparse
import json
from os.path import join
from datetime import datetime

import requests
import yaml
from pprint import pprint

def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  "Connecting to Connection Management "
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c','--config', help = 'Specifies config_demo.yaml file with the parameters: URL, TENANT, USER, PWD' ,
                        default='config.yamls')
    parser.add_argument('-i','--importcon', help = "Import connection")
    args = parser.parse_args()

    with open(args.config) as yamls:
        params = yaml.safe_load(yamls)

    auth = (params['TENANT']+'\\'+params['USER'], params['PWD'])
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    resturl = params['URL'] + '/app/datahub-app-connection/connections'

    if args.importcon :
        with open(join('connections',args.importcon),'r') as fp :
            connectjson = json.load(fp)
            #connectstr = fp.read()

    logging.info(f'POST \'{args.importcon}\': {resturl}')
    resp = requests.post(resturl, data= connectjson[0], auth=auth, headers=headers)

    pprint(resp)

if __name__ == '__main__':
    main()