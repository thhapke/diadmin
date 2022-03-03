# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import api


from urllib.parse import urljoin
import json

import requests

def gen():
    
    host = api.config.couchdb['connectionProperties']['host']
    user = api.config.couchdb['connectionProperties']['user']
    pwd = api.config.couchdb['connectionProperties']['password']
    db = api.config.couchdb['connectionProperties']['path']
    view = api.config.view
    
    url = urljoin(host, db) +'/'
    url = urljoin(url, view)
    
    selector = {'startkey':api.config.startkey,'endkey':api.config.endkey}
    if selector :
        selector = {k:"\""+str(v)+"\"" for k,v in selector.items()}
    
    r = requests.get(url, auth=(user, pwd),params = selector)
    
    json_content = r.content

    header = api.Record([host,db,view,api.config.startkey,api.config.endkey,0,0])
    header = {"diadmin.couchdb.header":header}
    api.logger.info(f"Header send:{header}")

    api.outputs.json.publish(json_content,header=header)


api.set_prestart(gen)