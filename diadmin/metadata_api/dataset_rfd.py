#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
########################################################
#### Using SAP Data Intelligence API Business Hub
#### Doc: https://api.sap.com/api/metadata/resource
########################################################
import logging
from urllib.parse import urljoin
import urllib
import json

import yaml
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF

from diadmin.utils.utils import get_system_id
from diadmin.metadata_api.connection import get_connections
from diadmin.metadata_api.container import get_containers

def export(connection,g=None):

    # RDF Graph
    if not g :
        g = Graph()

    ## Name spaces
    ndi = Namespace('https://www.sap.com/products/data-intelligence#')
    g.namespace_manager.bind('di',URIRef(ndi))

    nsys = Namespace(connection['host']+'/'+ connection['tenant']+'/')
    g.namespace_manager.bind('instance',URIRef(nsys))

    connection_filter = 'HANA_DB'
    container_filter = f"connectionType eq '{connection_filter}'"
    #container_filter = "contains(name,'HANA_Cloud_DQM')"

    # CONNECTIONS
    connections = get_connections(connection,filter_type=connection_filter)

    # CONTAINER
    containers = dict()
    get_containers(connection, containers,container_filter=container_filter)
    for c in containers.values() :
        # Connection level
        if c['parentId'] == 'connectionRoot' :
            g.add((nsys[c['connectionId']],RDF.type,ndi.Connection))
            continue
        qname = urllib.parse.quote(c['qualifiedName'],safe='')
        p_qname = urllib.parse.quote(c['parentQualifiedName'],safe='')
        g.add((nsys[qname],RDF.type,ndi.Container))
        g.add((nsys[qname],ndi.hasId,Literal(c['id'])))
        g.add((nsys[qname],ndi.hasName,Literal(c['name'])))
        g.add((nsys[qname],ndi.belongsConnection,nsys[c['connectionId']]))

        if c['name'] == c['qualifiedName']:
            g.add((nsys[c['connectionId']],ndi.hasContainer,nsys[qname]))
            g.add((nsys[qname],ndi.isChild,nsys[c['connectionId']]))
        else :
            g.add((nsys[qname],ndi.isChild,nsys[p_qname]))
            g.add((nsys[p_qname],ndi.hasChild,nsys[qname]))

    # DATASETS
    for name,container in containers.items() :
        datasets = get_datasets(connection, container['id'])
        for dataset in datasets :
            add_dataset_graphdb(gdb,dataset)
            # ATTRIBUTES
            ds = get_dataset_summary(connection, dataset['qualifiedName'], dataset['id'])
            if 'code' in ds and ds['code'] == '70016' :
                logging.warning(f"Read dataset summary for {dataset['qualifiedName']}: {ds['message']}")
                continue
            for att in ds['additionalInfo']['columns']:
                add_dataset_attribute_graphdb(gdb,dataset['id'],att)

            # TAGS
            dataset_tags = get_dataset_tags(connection, dataset['qualifiedName'], dataset['connectionId'])
            add_tag_relationship_graphdb(gdb,dataset['id'],dataset_tags)
    return g

#########
# MAIN
########
def main():
    logging.basicConfig(level=logging.INFO)

    with open('config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    connection = {'url':params['URL']+'/app/datahub-app-metadata/api/v1',
                  'host':params['URL'],'tenant':params['TENANT'],
                  'auth':(params['TENANT']+'\\'+ params['USER'],params['PWD'])}

    sysid = get_system_id(params['URL'],params['TENANT'])

    g = export(connection)
    print(g.serialize())

if __name__ == '__main__':
    main()