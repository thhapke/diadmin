#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
import logging
import argparse
import csv
import time
from urllib.parse import urljoin

import yaml
import json
from os import path

from diadmin.pipeline_api.runtime import start_batch
from diadmin.utils.utils import add_defaultsuffix

template_header = ["pipeline","name","user","password","config"]
template_type = [str,str,str,str,json.loads]

def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  "Starts batch of pipelines."
    help_config = 'Specifies config_demo.yaml file with the parameters: URL, TENANT, USER, PWD'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('batchfile', help = "json-batch-file")
    parser.add_argument('-c','--config', help = help_config,default='config.yamls')
    parser.add_argument('-n','--number',type=int, help='Max. number of parallel running pipelines',default=2)

    args = parser.parse_args()

    config_file = 'config.yaml'
    if args.config:
        config_file = add_defaultsuffix(args.config,'yaml')
    with open(config_file) as yamls:
        params = yaml.safe_load(yamls)

    conn = {'url': urljoin(params['URL'],'/app/pipeline-modeler/service/v1'),
            'tenant': params['TENANT'],
            'auth': (params['TENANT']+'\\'+params['USER'],params['PWD'])}

    batch_file = add_defaultsuffix(args.batchfile,'json')
    with open(path.join('batches',batch_file),mode='r',newline='\n') as jsonfp :
        batch = json.load(jsonfp)

    procs = start_batch(conn,batch = batch,max_procs=args.number)

    log_file = batch_file[:-4] + 'log'
    with open(log_file,'w') as fp:
        json.dump(procs,fp)


if __name__ == '__main__':
    main()