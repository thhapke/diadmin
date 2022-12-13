

from urllib.parse import urljoin
import json
import requests


#
# Callback of operator
#
def on_input(msg_id, header, data):

    # prepare
    host = api.config.httpconnection_basic['connectionProperties']['host']
    path = api.config.httpconnection_basic['connectionProperties']['path']
    user = api.config.httpconnection_basic['connectionProperties']['user']
    pwd = api.config.httpconnection_basic['connectionProperties']['password']
    keys = api.config.httpconnection_details['connectionProperties']['user'].split()
    proxy_authorization = api.config.httpconnection_details['connectionProperties']['password']

    restapi = f"/catalog/lineage/export"
    url = urljoin(host, path) + '/' + data.get()
    headers = {'X-Requested-With': 'XMLHttpRequest',
               'CallerGuid': keys[0],
               'CallerKey': keys[1],
               'apikey': keys[2],
               'apikeysecret': keys[3],
               'Proxy-Authorization': proxy_authorization,
               'Territory': api.config.territory}
               
    r = requests.get(url, headers=headers, auth=(user,pwd), params="{}")

    if r.status_code != 200:
        api.logger.error(f"Status code: {r.status_code}")
        return None

    opportunity = json.loads(r.text)
    # Select subset according to output port
    #opportunity = { k:v for k,v in opportunity.items() if k in ['prid', \
    #                'opportunityName', 'salesForceAccountID', 'accountPartyID',}
    #                'opportunityType']}
    
    columns = list(opportunity.keys())
    values = [list(opportunity.values())]

    table = api.Table(values)
    #type_ref = table.infer_dynamic_type(columns)
    #api.outputs.output.set_dynamic_type(type_ref)
    
    # Publish data
    api.outputs.output.publish(table, header=header)


api.set_port_callback("input", on_input)