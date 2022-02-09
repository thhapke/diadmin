#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
import logging
import copy
import time
from datetime import datetime
import json
from urllib.parse import urljoin

import requests
import yaml


start_template = {
    "src": "",
    "name": "",
    "executionType": "flow",
    "configurationSubstitutions": {},
    #"async": False
}

def get_graphs(connection) :
    restapi = "/runtime/graphs"
    url = connection['url'] + restapi
    logging.debug(f'Get runtime graphs URL: {url}')
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    resp = requests.get(url, headers=headers, auth=connection['auth'],verify=True)

    if resp.status_code != 200 :
        logging.error("Could not get runtime graphs!")
        return None

    rdata = json.loads(resp.text)
    data = dict()
    for r in rdata :
        #started = datetime.fromtimestamp(r['started']).strftime('%Y-%m-%d %H:%M:%S') if r['started'] >0  else '-'
        #submitted = datetime.fromtimestamp(r['submitted']).strftime('%Y-%m-%d %H:%M:%S') if r['submitted'] > 0 else '-'
        #stopped = datetime.fromtimestamp(r['stopped']).strftime('%Y-%m-%d %H:%M:%S') if r['stopped'] > 0 else '-'
        data[r['handle']] = {'pipeline':r['src'],'name':r['name'],'status': r['status'],
                             'submitted': r['submitted'],'started': r['started'],'stopped': r['stopped']}

    return data


def start_graph(connection,config) :

    restapi = "/runtime/graphs"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    r = requests.post(url, headers=headers, auth=connection['auth'], data = json.dumps(config))

    if r.status_code != 200:
        logging.error(f"Pipeline could not be started: {config['src']} - {r.status_code}\n{r.text}")
        return None

    r = json.loads(r.text)
    return {'pipeline': r['src'], 'name': r['name'],'user': r['user'],'tenant': r['tenant'],
            'status': r['status'],'config':r['configurationSubstitutions'],'handle':r['handle'],
            'submitted': r['submitted'],'started': r['started'],'stopped': r['stopped']}



def start_batch(connection, batch, max_procs = 2) :

    processing = True
    num_procs = 0
    index = 0
    procs =dict()
    auths = {connection['auth']}
    while(processing) :
        # If all pipelines have been processed stop process
        if index == len(batch):
            processing = False
            break
        # If there available resources start new pipeline
        if num_procs < max_procs :
            if 'sleep_time' in batch[index]:
                logging.info(f"Sleep for {batch[index]['sleep_time']}s")
                time.sleep(batch[index]['sleep_time'])
                index +=1
                continue

            conn = connection.copy()
            if "user" in batch[index] :
                user_auth = (batch[index]['tenant']+'\\'+ batch[index]['user'],batch[index]['password'])
                conn['auth'] = user_auth
                auths.add(user_auth)

            rec = start_graph(conn, batch[index])
            if rec  :
                logging.info(f"{index} - Pipeline started: {rec['name']}")
                rec['batch_index'] = index
                procs[rec['handle']] = rec
                num_procs +=1
            index +=1  # in case of failing start of pipeline - it is skipped
            time.sleep(5)

        # Check runtime graphs for changes, update procs and free up proc resources
        runtime_graphs = list()
        for auth in auths :
            conn = {'url': connection['url'],'auth': auth}
            runtime_graphs.append(get_graphs(conn))

        for runtime_graph in runtime_graphs:
            if runtime_graph == None :
                logging.error(f'Runtime graph is None in {runtime_graphs}')
                continue
            for h,rg in runtime_graph.items():
                # filter runtime graphs
                if (not h in procs.keys()) or (procs[h]['status'] in ['dead','completed']) :
                    continue
                # update procs
                #logging.debug(f"Check for update: {h}")
                procs[h]['status'] = rg['status']
                procs[h]['submitted'] = rg['submitted']
                procs[h]['started'] = rg['started']
                procs[h]['stopped'] = rg['stopped']

                if rg['status'] in ['dead','completed'] :
                    num_procs -=1
                    logging.info(f"Pipeline stopped: {rg['name']} -  {rg['status']}")
                    logging.info(f"New proc resource available: {num_procs}/{max_procs}")


    # Wait until all processes are complete

    running = True
    while(running) :

        running = False
        runtime_graphs = list()
        for auth in auths :
            conn = {'url': connection['url'],'auth': auth}
            runtime_graphs.append(get_graphs(conn))

        for runtime_graph in runtime_graphs:
            for h,rg in runtime_graph.items():
                # filter runtime graphs
                if (not h in procs.keys()) or (procs[h]['status'] in ['dead','completed']) :
                    continue
                # update procs
                #logging.info(f"Check for update: {h}")
                procs[h]['status'] = rg['status']
                procs[h]['submitted'] = rg['submitted']
                procs[h]['started'] = rg['started']
                procs[h]['stopped'] = rg['stopped']

                if not rg['status'] in ['dead','completed']:
                    running = True

    logging.info("Batch process ended.")

    return procs


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    with open('config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    conn = {'url': urljoin(params['URL'] , '/app/pipeline-modeler/service'),
            'auth': (params['TENANT'] + '\\' + params['USER'], params['PWD'])}

    pipelines1 = [{ 'pipeline':'utils.conf_datagen', 'configuration':{'name':'extstart-1'}}]
    pipelines2 = [{ 'pipeline':'utils.conf_datagen', 'configuration':{'name':'BS-1'}},
                  { 'pipeline':'utils.conf_datagen', 'configuration':{'name':'BS2-2'}}]
    pipelines4 = [{ 'pipeline':'utils.conf_datagen', 'configuration':{'name':'BS-1'}},
                  { 'pipeline':'utils.conf_datagen', 'configuration':{'name':'BS2-2'}},
                  { 'pipeline':'utils.conf_datagen', 'configuration':{'name':'BS2-3'}},
                  { 'pipeline':'utils.conf_datagen', 'configuration':{'name':'BS2-4'}}]


    procs = start_batch(conn, batch=pipelines4)

    print(json.dumps(procs,indent=4))