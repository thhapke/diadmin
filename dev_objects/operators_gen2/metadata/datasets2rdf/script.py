import logging
import json
import os

from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS


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

    # Base URL
    dicatalog = 'https://vsystem.xxx.ondemand.com/default/catalog'

    for dataset in datasets:
        dataset_uri = URIRef(f"{dicatalog}/dataset/{dataset['metadata']['connectionId']}{dataset['metadata']['uri']}")
        dataset_type = dataset['metadata']['type'].capitalize()
        g.add((dataset_uri, RDF.type, dimd[dataset_type]))

        # COLUMNS
        for column in dataset['columns']:
            column_uri = URIRef(dataset_uri+'/'+column['name'])
            g.add((dataset_uri, dimd.hasColumn, column_uri))
            g.add((dataset_uri, dimd.DataType, Literal(column['type'])))
            g.add((dataset_uri, dimd.TemplateDataType, Literal(column['templateType'])))
            if 'length' in column:
                g.add((dataset_uri, dimd.FieldLength, Literal(column['length'])))

        # TAGS
        if 'tags' in dataset:
            if 'tagsOnDataset' in dataset['tags']:
                for dtag in dataset['tags']['tagsOnDataset']:
                    hierarchy_uri = f"{dicatalog}/hierarchy/{dtag['hierarchyName']}"
                    for tag in dtag['tags']:
                        tag_path = tag['tag']['path'].replace('.', '/')
                        tag_path = tag_path.replace(' ', '%20')
                        tag_uri = URIRef(f"{hierarchy_uri}/{tag_path}")
                        g.add((dataset_uri, dimd.hasTag, tag_uri))
            if 'tagsOnAttribute' in dataset['tags']:
                for atag in dataset['tags']['tagsOnAttribute']:
                    column_uri = URIRef(dataset_uri + '/' + atag['attributeQualifiedName'])
                    for tag in atag['tags']:
                        hierarchy_uri = f"{dicatalog}/hierarchy/{tag['hierarchyName']}"
                        for tag2 in tag['tags']:
                            tag_path = tag2['tag']['path'].replace('.', '/')
                            tag_path = tag_path.replace(' ', '%20')
                            tag_uri = URIRef(f"{hierarchy_uri}/{tag_path}")
                            g.add((column_uri, dimd.hasTag, tag_uri))

        # LINEAGE
        if 'lineage' in dataset and isinstance(dataset['lineage'],dict):
            logging.info(f"Lineage of dataset: {dataset['metadata']['uri']}")

            for pcn in dataset['lineage']['publicComputationNodes']:
                for transform in pcn['transforms']:
                    for computation in transform['datasetComputation']:
                        lineage_to = URIRef(f"{dicatalog}/lineageTo/{computation['computationType']}")
                        lineage_from = URIRef(f"{dicatalog}/lineageFrom/{computation['computationType']}")
                        in_uris = [URIRef(f"{dicatalog}/dataset/{ind['externalDatasetRef']}")
                                   for ind in computation['inputDatasets']]
                        out_uris = [URIRef(f"{dicatalog}/dataset/{ind['externalDatasetRef']}")
                                    for ind in computation['outputDatasets']]
                        for i in in_uris:
                            for o in out_uris:
                                g.add((i, lineage_to, o))
                                g.add((o, lineage_from, i))

    rdf_str = g.serialize()
    api.outputs.output.publish(rdf_str, header=header)


api.set_port_callback("input", on_input)
