# Mock apis needs to be commented before used within SAP Data Intelligence
from diadmin.dimockapi.mock_api import api

import logging
import json
import io

from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD
from owlrl import RDFS_Semantics, DeductiveClosure

map_xsd_datatypes = {
    "TIME": "xsd:time",
    "DATE": "xsd:date",
    "DECIMAL": "xsd:decimal",
    "BINARY": "xsd:base64Binary",
    "INTEGER": "xsd:integer",
    "STRING": "xsd:string",
    "DATETIME": "xsd.dateTime",
    "BOOLEAN": "xsd.boolean",
    "FLOAT": "xsd:float",
    "DOUBLE": "xsd:double"
}


#
# DATA INPUT
#
def on_input(msg_id, header, data):

    datasets = json.loads(data.get())

    # create Graph
    g = Graph()
    dimd = Namespace("https://www.sap.com/products/data-intelligence#")
    g.bind("dimd", dimd)

    g.add((dimd.Table, RDFS.subClassOf, dimd.Dataset))
    g.add((dimd['File.csv'], RDFS.subClassOf, dimd.Dataset))


    # Base URL
    dicatalog = 'https://vsystem.xxx.ondemand.com/default/catalog'

    for dataset in datasets:
        dataset_uri = URIRef(f"{dicatalog}/dataset/{dataset['metadata']['connectionId']}{dataset['metadata']['uri']}")
        dataset_type = dataset['metadata']['type'].capitalize()
        g.add((dataset_uri, RDF.type, dimd[dataset_type]))
        g.add((dataset_uri, RDFS.label, Literal(dataset['metadata']['name'])))

        # comment = descriptions
        if "descriptions" in dataset['metadata']:
            for d in dataset['metadata']['descriptions']:
                if d['type'] == 'SHORT':
                    g.add((dataset_uri, RDFS.comment, Literal(d['value'])))
                    break

        # primary keys
        # unique_keys = list()
        if "uniqueKeys" in dataset["metadata"]:
            for uk in dataset["metadata"]["uniqueKeys"]:
                for pk in uk["attributeReferences"]:
                    column_uri = URIRef(dataset_uri + '/' + pk)
                    g.add((dataset_uri, dimd.primaryKey, column_uri))
                    g.add((column_uri, dimd.key, Literal(True)))
                    # only necessary because RDFLIB SPARQL EXIST does not work
                    # unique_keys.append(column_uri)

        # COLUMNS
        for column in dataset['columns']:
            column_uri = URIRef(dataset_uri+'/'+column['name'])
            g.add((column_uri, RDFS.label, Literal(column['name'])))
            g.add((dataset_uri, dimd.column, column_uri))
            g.add((column_uri, RDF.type, dimd.Column))
            g.add((column_uri, dimd.datatype, Literal(column['type'])))
            if column['type'] in map_xsd_datatypes:
                g.add((column_uri, RDFS.range, Literal(map_xsd_datatypes[column['type']])))
            else:
                api.logger.warning(f"No XSD mapping for {column['type']}")
            g.add((column_uri, dimd.templateDataType, Literal(column['templateType'])))
            if 'length' in column:
                g.add((column_uri, dimd.length, Literal(column['length'])))
            if 'precision' in column:
                g.add((column_uri, dimd.precision, Literal(column['precision'])))
            if 'scale' in column:
                g.add((column_uri, dimd.scale, Literal(column['scale'])))
            if "descriptions" in column:
                for d in column['descriptions']:
                    if d['type'] == 'SHORT':
                        g.add((column_uri, RDFS.comment, Literal(d['value'])))
                        break

        # TAGS
        if 'tags' in dataset:
            if 'tagsOnDataset' in dataset['tags']:
                for dtag in dataset['tags']['tagsOnDataset']:
                    hierarchy_uri = f"{dicatalog}/hierarchy/{dtag['hierarchyName']}"
                    for tag in dtag['tags']:
                        tag_path = tag['tag']['path'].replace('.', '/')
                        tag_path = tag_path.replace(' ', '%20')
                        tag_uri = URIRef(f"{hierarchy_uri}/{tag_path}")
                        g.add((dataset_uri, dimd.tag, tag_uri))
            if 'tagsOnAttribute' in dataset['tags']:
                for atag in dataset['tags']['tagsOnAttribute']:
                    column_uri = URIRef(dataset_uri + '/' + atag['attributeQualifiedName'])
                    for tag in atag['tags']:
                        hierarchy_uri = f"{dicatalog}/hierarchy/{tag['hierarchyName']}"
                        for tag2 in tag['tags']:
                            tag_path = tag2['tag']['path'].replace('.', '/')
                            tag_path = tag_path.replace(' ', '%20')
                            tag_uri = URIRef(f"{hierarchy_uri}/{tag_path}")
                            g.add((column_uri, dimd.tag, tag_uri))
                            if tag['hierarchyName'] == 'AlternativeLabels':
                                g.add((column_uri, RDFS.label, Literal(tag2['tag']['name'])))

        # LINEAGE
        if 'lineage' in dataset and isinstance(dataset['lineage'], dict):
            logging.info(f"Lineage of dataset: {dataset['metadata']['uri']}")
            for pcn in dataset['lineage']['publicComputationNodes']:
                for transform in pcn['transforms']:
                    for computation in transform['datasetComputation']:
                        if 'inputDatasets' not in computation or 'outputDatasets' not in computation:
                            api.logger.info(f"No inputDatasets or outputDatasets in lineage for {dataset['metadata']['uri']}")
                            continue
                        else:
                            in_uris = [URIRef(f"{dicatalog}/dataset/{ind['externalDatasetRef']}")
                                       for ind in computation['inputDatasets']]
                            out_uris = [URIRef(f"{dicatalog}/dataset/{ind['externalDatasetRef']}")
                                        for ind in computation['outputDatasets']]
                        for i in in_uris:
                            for o in out_uris:
                                g.add((i, dimd.lineage, o))
                                g.add((i, dimd.computationType, Literal(computation['computationType'])))
                                g.add((o, dimd.impact, i))

    # Expand graph for RDFS semantics
    DeductiveClosure(RDFS_Semantics).expand(g)
    data = io.BytesIO(g.serialize().encode("utf8"))
    #api.outputs.output.publish(data, -1, header=header)
    api.outputs.output.publish(data, header=header)


api.set_port_callback("input", on_input)
