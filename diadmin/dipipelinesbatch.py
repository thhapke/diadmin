import logging
import argparse
from urllib.parse import urljoin

import yaml
import json
from os import path

from diadmin.pipeline_api.runtime import start_batch
from diadmin.utils.utils import add_defaultsuffix

def main() :
    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  "Starts pipelines from batch with maximum number of running pipelines."
    help_config = 'Specifies config_demo.yaml file with the parameters: URL, TENANT, USER, PWD'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('batchfile', help = "json-batch-file")
    parser.add_argument('-n','--number',type=int, help='Max. number of parallel running pipelines',default=2)
    parser.add_argument('-s','--sleep_time',type=int, help='Idle time for each loop')
    parser.add_argument('-c','--config', help = help_config,default='config.yamls')

    args = parser.parse_args()

    config_file = 'config.yaml'
    if args.config:
        config_file = add_defaultsuffix(args.config,'yaml')
    with open(config_file) as yamls:
        params = yaml.safe_load(yamls)

    conn = {'url': urljoin(params['URL'] , '/app/pipeline-modeler/service/v1'),
            'auth':(params['TENANT']+'\\'+ params['USER'],params['PWD'])}

    batch_file = add_defaultsuffix(args.batchfile,'json')
    with open(batch_file) as fp:
        batch = json.load(fp)

    procs = start_batch(conn,pipelines=batch,max_procs=args.number,sleep_time=args.sleep_time)

    log_file = batch_file[:-4] + 'log'
    with open(log_file,'w') as fp:
        json.dump(procs,fp)


if __name__ == '__main__':
    main()