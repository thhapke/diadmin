#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

import urllib
import logging
from os import path

import yaml
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS


from diadmin.metadata_api.catalog import download_hierarchies


def add_hierarchy_rdf(connection, g=None):
    # RDF Graph
    if not g:
        g = Graph()

    # Name spaces
    ndi = Namespace('https://www.sap.com/products/data-intelligence#')
    g.namespace_manager.bind('di', URIRef(ndi))

    instance_url = connection['host'] + '/' + connection['tenant']
    ninstance = Namespace(instance_url)
    g.namespace_manager.bind('instance', URIRef(ninstance))

    catalog_url = instance_url+'/'+'catalog'
    ncatalog = Namespace(catalog_url+ '/' )
    catalog = URIRef(catalog_url)
    g.namespace_manager.bind('catalog', URIRef(ncatalog))

    g.add((ninstance.metadata_explorer, ndi.hasCatalog, catalog))
    g.add((catalog, RDF.type, ndi.catalog))

    # HIERARCHIES
    hierarchies = download_hierarchies(connection)
    for t in hierarchies.values():
        levels = len([c for c in t['path'] if c == '/'])
        if levels == 0:
            g.add((catalog, ndi.hasTagHierarchy, ncatalog[t['name']]))
            g.add((ncatalog[t['name']], RDF.type, ndi.Hierarchy))
            g.add((ncatalog[t['name']], ndi.hasId, Literal(t['hierarchy_id'])))
            g.add((ncatalog[t['name']], RDFS.comment, Literal(t['description'])))
            g.add((ncatalog[t['name']], RDFS.label, Literal(t['name'])))
        else:
            qname = urllib.parse.quote(t['path'])
            node = ncatalog[qname]
            g.add((ncatalog[qname], RDF.type, ndi.Tag))
            g.add((ncatalog[qname], RDFS.comment, Literal(t['description'])))
            g.add((ncatalog[qname], RDFS.label, Literal(t['name'])))
            g.add((ncatalog[t['hierarchy_name']], ndi.hasTag, ncatalog[qname]))
            if levels > 1:
                pqname = urllib.parse.quote(t['parent_path'])
                g.add((ncatalog[qname], RDFS.subClassOf, ncatalog[pqname]))
                #g.add((nsys[qname], ndi.hasParentTag, nsys[pqname]))
                #g.add((nsys[pqname], ndi.hasChildTag, nsys[qname]))

    return g


#
# MAIN
#
def main():

    logging.basicConfig(level=logging.INFO)

    with open('config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    connection = {'url': params['URL']+'/app/datahub-app-metadata/api/v1',
                  'host': params['URL'], 'tenant': params['TENANT'],
                  'auth': (params['TENANT']+'\\' + params['USER'], params['PWD'])}

    g = add_hierarchy_rdf(connection)
    g.serialize(destination=path.join('rdf_resources', 'hierarchy.ttl'))


if __name__ == '__main__':
    main()
