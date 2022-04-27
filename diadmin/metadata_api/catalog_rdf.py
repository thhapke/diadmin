#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

import requests
import json
import urllib
import logging
import yaml
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF


from diadmin.metadata_api.catalog import download_hierarchies

def add_hierarchy_rdf(connection,g=None) :
    # RDF Graph
    if not g :
        g = Graph()

    ## Name spaces
    ndi = Namespace('https://www.sap.com/products/data-intelligence#')
    g.namespace_manager.bind('di',URIRef(ndi))

    nsys = Namespace(connection['host'] +'/' + connection['tenant'])
    g.namespace_manager.bind('instance',URIRef(nsys))

    g.add((nsys.metadata_explorer,ndi.hasCatalog,nsys.catalog))
    g.add((nsys.catalog,RDF.type,ndi.catalog))

    # HIERARCHIES
    hierarchies = download_hierarchies(connection)
    for t in hierarchies.values():
        levels = len([c for c in t['path'] if c =='/'])
        if levels == 0 :
            g.add((nsys.catalog,ndi.hasTagHierarchy,nsys[t['name']]))
            g.add((nsys[t['name']],RDF.type,ndi.Hierarchy))
            g.add((nsys[t['name']],ndi.hasId,Literal(t['hierarchy_id'])))
        else :
            #qname = urllib.parse.quote(t['path'],safe='')
            qname= urllib.parse.quote(t['path'],safe='')
            g.add((nsys[qname],RDF.type,ndi.Tag))
            g.add((nsys[t['hierarchy_name']],ndi.hasTag,nsys[qname]))
            if levels > 1 :
                pqname = urllib.parse.quote(t['parent_path'],safe='')
                g.add((nsys[qname],ndi.hasParentTag,nsys[pqname]))
                g.add((nsys[pqname],ndi.hasChildTag,nsys[qname]))

    return g

def main() :

    logging.basicConfig(level=logging.INFO)

    with open('config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    connection = {'url':params['URL']+'/app/datahub-app-metadata/api/v1',
                  'host':params['URL'],'tenant':params['TENANT'],
                  'auth':(params['TENANT']+'\\'+ params['USER'],params['PWD'])}

    g = add_hierarchy_rdf(connection)
    print(g.serialize())

if __name__ == '__main__' :
    main()