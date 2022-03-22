from urllib.parse import urljoin
import json

import requests
import yaml


def get_doc(conn,id) :
    url = urljoin(conn['URL'], conn['DB'], id)
    r = requests.get(url, auth=(conn['USER'], conn['PWD']))
    return json.loads(r.content)

def get_data_dbs(conn):
    url = urljoin(conn['URL'], '_all_dbs')
    #url = urljoin(conn['URL'], conn['PATH'])
    r = requests.get(url, auth=(conn['USER'], conn['PWD']))
    rcontent = json.loads(r.content)
    return [ d for d in rcontent if d[0] != '_']

def get_view_data(conn,view=None,selector=None):
    url = urljoin(conn['COUCHDB'], view)
    if selector :
        selector = {k:"\""+str(v)+"\"" for k,v in selector.items()}
    r = requests.get(url, auth=(conn['USER'], conn['PWD']),params = selector)
    return json.loads(r.content)


def main() :
    with open("couchdb.yaml") as yamls:
        conn = yaml.safe_load(yamls)
    conn['COUCHDB'] =  urljoin(conn['URL'], conn['DB']) +'/'


    #dbs = get_data_dbs(conn)
    #print(dbs)

    #doc = get_doc(conn,"access-control-server_7999_2021_08_06")
    #print(doc)

    view = "_design/sap_analytics/_view/visits"
    #selector = {'startkey':"\"2022/02/19\"",'endkey':"\"2022/02/20\""}
    selector = {'startkey':"2022/02/19",'endkey':"2022/02/20"}
    docs = get_view_data(conn,view,selector)
    print(docs)


    #docs = find_docs(conn,selector)
    #print(docs)

    #couch = couchdb.Server("https://sap_poc:sap__checkIn_2022@"+conn['URL'])
    #db1 = couch.create('saptest')
    #db2 = couch['saptest']


if __name__ == '__main__':
    main()