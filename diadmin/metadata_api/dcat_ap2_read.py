#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

import logging
import re
import os
from pprint import pprint

import yaml
from rdflib import Graph, term, URIRef

logging.basicConfig(level=logging.INFO)

def parseRDF(file) :
    g = Graph()
    g.parse(file)
    #print(g.serialize(format="turtle"))
    return g

def save_turtle(g,filename) :
    with open(filename,"w") as outfile:
        outfile.write(g.serialize(format="turtle"))

def replace_suffix(file,suffix) :
    return os.path.splitext(file)[0] + '.' + suffix

def str_record(rec) :
    recstr = ''
    if 'subj' in rec :
        recstr = f"{rec['subj']['value']} ({rec['subj']['type']})"
    recstr += ' -- '
    if 'pred' in rec :
        recstr += f"{rec['pred']['value']} ({rec['pred']['type']})"
    recstr += ' --> '
    if 'obj' in rec :
        recstr += f"{rec['obj']['value']} ({rec['obj']['type']})"
    if len(rec['lang']) > 0 :
        recstr += f"(@{rec['lang']})"
    return recstr

def query_tripels(g,query) :
    logging.info(f"Query: {query}")
    qresult = g.query(query)
    rlist = []
    for rs in qresult:
        rrec = { label:dict() for label in rs.labels}
        rrec['lang'] = list()
        for label in rs.labels :
            if not rs[label] :
                rrec[label]['type'] = None
            elif isinstance(rs[label] ,term.BNode) :
                rrec[label]['type'] = 'bnode'
                rrec[label]['node'] = rs[label]
            elif isinstance(rs[label] ,term.Literal) :
                rrec[label]['type'] = 'literal'
                rrec[label]['lang'] = rs[label].language
                rrec[label]['value'] = rs[label].value
                rrec['lang'].append(rs[label].language)
                rrec[label]['node'] = rs[label]
            elif isinstance(rs[label] ,term.URIRef) :
                rrec[label]['type'] = 'uriref'
                rrec[label]['value'] = rs[label].n3(g.namespace_manager).strip('<>')
                rrec[label]['node'] = rs[label]
            else :
                rrec[label]['type'] = 'notused'
                rrec[label]['node'] = rs[label]
        rlist.append(rrec)
        if len(rrec['lang']) >1 :
            logging.warning(f'More than one language in result: {rrec} ')
    logging.info(f'#records: {len(rlist)}')
    return rlist


#rdf_file = '../dev_data/dcat-ap_2.0.1.ttf'
rdf_file = '../testdata/availability_1_0.rdf'
rdf_file = 'https://www.dcat-ap.de/def/plannedAvailability/1_0.rdf'
g = parseRDF(rdf_file)

# Get file namespace
ns_file = ''
for ns_prefix, namespace in g.namespaces():
    if '' == ns_prefix:
        ns_file = namespace
        break

root = ns_file[ns_file.rfind('/')+1:]
qresult = g.query("SELECT  ?subj ?obj WHERE { ?subj skos:prefLabel  ?obj}")

tree = {'name':None,'description':None,'nodes':[]}
for r in qresult :
    m = re.sub(ns_file,'', r['subj'] )
    if m == ns_file :
        continue
    elif not m:
        tree['name'] = root
        tree['description'] = r['obj'].n3(g.namespace_manager).strip('<>').split('@')[0]
    else :
        nr = {'name':m[1:],'description':r['obj'].value,'nodes':[]}
        tree['nodes'].append(nr)
pprint(tree)


#rlist = query_tripels(g,query)

# Remove Optional Terms
#rlist = query_tripels(g,"SELECT  ?subj ?obj WHERE { ?subj vann:usageNote ?obj}")
#for r in rlist:
#    if re.match('Optional property',r['obj']['value']) or re.match('Optional class',r['obj']['value']):
#        g.remove((r['subj']['node'], None, None))

#rlist = query_tripels(g,"SELECT DISTINCT ?subj ?obj WHERE { ?subj rdfs:label ?obj}")



#for subj, pred, obj in g:
#    # Check if there is at least one triple in the Graph
#    if (subj, pred, obj) not in g:
#        print('error')

