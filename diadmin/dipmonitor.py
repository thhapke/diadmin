#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#


import logging
import argparse
import json
from datetime import datetime

import requests
import yaml
from pprint import pprint

def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  "Monitors SAP Data Intelligence pipelines of a user"
    parser = argparse.ArgumentParser(description=description)
    help_config = 'Specifies config.yaml file with the parameters like URL, TENANT, USER, PWD'
    parser.add_argument('-c','--config', help = help_config,default='config.yamls')
    parser.add_argument('-u','--user', help = 'Userlist ',default='config.yamls')
    args = parser.parse_args()

    with open(args.config) as yamls:
        params = yaml.safe_load(yamls)


    auth = (params['TENANT']+'\\'+params['USER'], params['PWD'])
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    resturl = params['URL'] + '/app/pipeline-modeler/service/v1/runtime/graphs'

    resp = requests.get(resturl, auth=auth, headers=headers,verify = True)
    data = list()
    if resp.status_code == 200 :
        rdata = json.loads(resp.text)
        for r in rdata :
            started = datetime.fromtimestamp(r['started']).strftime('%Y-%m-%d %H:%M:%S') if r['started'] >0  else '-'
            submitted = datetime.fromtimestamp(r['submitted']).strftime('%Y-%m-%d %H:%M:%S') if r['submitted'] > 0 else '-'
            stopped = datetime.fromtimestamp(r['stopped']).strftime('%Y-%m-%d %H:%M:%S') if r['stopped'] > 0 else '-'
            data.append({'Pipeline': r['src'], 'Status': r['status'],'Submitted': submitted,'Started': started,
                         'Stopped': stopped,'Messages':r['message']})

    pprint(data)

if __name__ == '__main__':
    main()