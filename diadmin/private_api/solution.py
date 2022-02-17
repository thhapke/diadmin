import logging
import json
from urllib.parse import urljoin

import requests



def build_solution(connection,config) :

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
