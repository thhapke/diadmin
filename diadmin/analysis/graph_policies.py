#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#


import logging
import csv
import json
from copy import deepcopy
import re

import yaml
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt

def get_default_resource_classes() :
    return  {
        "connectionConfiguration":"admin",
        "connection":"admin",
        "connectionContent":"dev_data",
        "app.datahub-app-dev_data.qualityDashboard":"metadata",
        "app.datahub-app-core.connectionCredentials":"metadata",
        "app.datahub-app-dev_data.profile":"metadata",
        "app.datahub-app-dev_data.qualityRulebook":"metadata",
        "app.datahub-app-dev_data.system":"metadata",
        "app.datahub-app-dev_data.catalog":"metadata",
        "app.datahub-app-dev_data.glossary":"metadata",
        "app.datahub-app-dev_data.tagHierarchy":"metadata",
        "app.datahub-app-dev_data.qualityRule":"metadata",
        "app.datahub-app-dev_data.preparation":"metadata",
        "app.datahub-app-dev_data.publication":"metadata",
        "application":"application",
        "systemManagement":"admin",
        "certificate":"admin",
        "connectionCredential":"admin"
    }

def get_default_color_map() :
    return {
        "admin":"black",
        "metadata":"green",
        "application":"orange",
        "dev_data":"blue",
        "multiple":"grey"
    }

##### PRINT
def print_edges(graph,att_filter=None,node_filter=None) :
    print("=== EDGES ===")
    num_passed_edges = 0
    for fn,tn,att in graph.edges(data=True) :
        passed = True
        if att_filter :
            for f,v in att_filter.items() :
                if att[f] != v :
                    passed = False
                    break
        if node_filter :
            if not (fn in node_filter or tn in node_filter) :
                passed = False
        if passed :
            num_passed_edges += 1
            print(f"{graph.nodes[fn]['num_id']}/{fn} -> {graph.nodes[tn]['num_id']}/{tn}: {att['type']}")
    logging.info(f"Filtered: {num_passed_edges}/{len(graph.edges)}")


def tag_nodes_by_name(graph,name_filter,new_attribute,successor_nodes = True,remove_untagged = False) :
    tagged_nodes = list()
    for node in graph.nodes :
        if re.match(name_filter,node):
            graph.nodes[node][new_attribute] = True
            tagged_nodes.append(node)
        else :
            graph.nodes[node][new_attribute] = False

    if successor_nodes :
        for node in tagged_nodes :
            tag_successors(graph,node,new_attribute)

    orig_len = len(graph.nodes)

    if remove_untagged :
        remove_nodes = [ n for n,att in graph.nodes(data=True) if not att[new_attribute] ]
        for n in remove_nodes :
            graph.remove_node(n)

    logging.info(f'Tagging of nodes: {orig_len} -> {len(graph.nodes)}')


def tag_successors(graph,node,new_tag) :
    pn = [n for n in graph.predecessors(node)]
    for n in pn :
        graph.nodes[n][new_tag] = True
        tag_successors(graph,n,new_tag)

def filter_nodes(graph, att_filter=None, node_filter=None) :

    filtered_nodes = list()
    for node,att in graph.nodes(data=True) :
        # Filters
        if node_filter and not (node in node_filter ) :
            continue
        if att_filter :
            passed = True
            for f,v in att_filter.items() :
                if att[f] != v :
                    passed = False
                    break
            if not passed :
                continue
        # Passed
        filtered_nodes.append(node)

    logging.info(f"Filtered: {len(filtered_nodes)}/{len(graph.nodes)}")
    return filtered_nodes

def print_node_connection(graph,filter,nodes=None):
    print("=== NODE CONNECTIONS ===")
    if not nodes :
        nodes = graph.nodes
    num_passed_nodes = 0
    for p in nodes:
        # filter nodes
        passed = True
        for f,v in filter.items() :
            if graph.nodes[p][f] != v :
                passed = False
                break

        #
        if passed :
            num_passed_nodes += 1
            sn = [n for n in graph.successors(p) if not n == 'root']
            pn = [n for n in graph.predecessors(p) if not n == 'root']
            print(f"{graph.nodes[p]['num_id']:>3}: {p} MAX: {graph.nodes[p]['max_distance']:>3} (in: {pn}  out: {sn})")
    logging.info(f"Filtered: {num_passed_nodes}/{len(nodes)}")

def max_length(graph,node,length) :
    pn = [n for n in graph.predecessors(node)]
    if len(pn) == 0 :
        if node == 'root' :
            return length
        else :
            return length +1

    max_l = length+1
    for n in pn :
        path_l = max_length(graph,n,length+1)
        if path_l > max_l :
            max_l = path_l
    return max_l

# longest path from root to node
def compute_levels(graph):
    '''
    Calculates the maximum direct path to root
    :param graph: networkx directed graph
    :return: None
    '''
    max_graph_distance = 2
    for n,att in graph.nodes(data=True) :
        if n == 'root' :
            graph.nodes['root']['level'] = 0
            continue
        maxs = max_length(graph,n,1)
        graph.nodes[n]['level'] = maxs-1
        logging.debug(f"{graph.nodes[n]['level']} Level of node {n}")
        if maxs > max_graph_distance :
            max_graph_distance = maxs

    logging.info(f'Network levels/depth: {max_graph_distance}')
    graph.nodes['root']['max_level'] = max_graph_distance

# Calculates the position of the nodes
def calc_pos(graph, nodes, width=1., height = 1.0, y_width_band = 0.01):
    '''
    Calculates the position of the nodes in a box (1.,1)
    :param G: networkx graph
    :param nodes: list of nodes
    :param width: plot width
    :param height: plot height
    :param y_width_band: scatters nodes within this band
    :return: node positions
    '''

    # set all exposed nodes to max_level
    for n,att in graph.nodes(data=True) :
        if att['exposed'] :
            graph.nodes[n]['level'] = graph.nodes['root']['max_level']-1

    dy = height/graph.nodes['root']['max_level']
    ddy = y_width_band * height
    pos = {'root':(width*0.5,1.)}
    for level in range(1,graph.nodes['root']['max_level']) :
        nodes_level = [n for n,att in graph.nodes(data=True) if att['level']==level]

        if len(nodes_level) == 0 :
            logging.info(f"No nodes in level: {level}")
            continue
        dx = width/len(nodes_level)
        x = dx*0.5 - dx
        i_1 = -1.
        for node in nodes_level :
            i_1 = -1. if i_1 == 1 else 1.
            y = 1.0 - (dy * level) + (ddy *  i_1)
            x += dx
            pos[node]=(x,y)
    return pos

# Draws network with distance levels
def draw_graph(graph,resource_classes,color_map,filename = 'analysis.png') :
    '''
    Draws network with horizontal levels with a additional 'root'-node
    :param graph: network graph
    :return: None
    '''

    # filter nodes and edges

    #policy_path_edges = [(f,t) for f,t,att in graph.edges(dev_data=True) if att['type'] =='policy_policy']
    #root_path_edges = [(f,t) for f,t,att in graph.edges(dev_data=True) if (att['type'] =='root_policy' and graph.nodes[t]['path_node'] == True)]

    #policy_path_nodes = [n for n,att in graph.nodes(dev_data=True) if att['path_node']==True]


    display_nodes = [n for n,att in graph.nodes(data=True) if not n == 'root' ]
    display_edges = [(f,t) for f,t,att in graph.edges(data=True) if  f in display_nodes and t in display_nodes]

    pos = calc_pos(graph,display_nodes)

    #labels
    labels = { p: graph.nodes[p]['num_id'] for p in display_nodes}
    nx.draw_networkx_labels(graph,pos,labels,font_size=8,font_color='w')

    #nodes
    shape_map ={True:'D',False:'o'}
    node_color =  [color_map[graph.nodes[n]['policyClass']] for n in display_nodes]
    options = {"node_color": node_color, "node_size": 120, "alpha": 0.9}

    exposed_nodes = [n for n in display_nodes if graph.nodes[n]['exposed'] and not n == "root"]
    non_exposed_nodes =  [n for n in display_nodes if not graph.nodes[n]['exposed'] and not n == "root"]

    node_color =  [color_map[graph.nodes[n]['policyClass']] for n in non_exposed_nodes]
    options["node_color"] = node_color
    nx.draw_networkx_nodes(graph, pos, non_exposed_nodes, node_shape = 'o',**options)
    node_color =  [color_map[graph.nodes[n]['policyClass']] for n in exposed_nodes]
    options["node_color"] = node_color
    nx.draw_networkx_nodes(graph, pos, exposed_nodes, node_shape = 'D',**options)

    #edges
    nx.draw_networkx_edges(graph,pos,display_edges, width=0.5,edge_color='cornflowerblue')
    #nx.draw_networkx_edges(graph,pos,root_path_edges, width=1,edge_color='dimgray')

    plt.savefig(filename)
    plt.legend()
    plt.show()


# network of analysis
def build_network(policies,resource_classes) :
    '''
    Build network form analysis with a root node that connects to all 'exposed' nodes. In addition edges
    connect policy nodes that inherit from policy nodes
    :param policies: List of analysis created by calling 'vctl policy get <id>' interatively
    :return: networkx graph
    '''
    graph = nx.DiGraph()

    # Hierarchical graph needs a root graph
    graph.add_node('root',tenant=policies[0]['tenant'],path_node=True,num_id = 0,exposed=False,
               resources=list(),inherited=list(),policyClass = 'multiple')

    # Add all analysis as node and connect 'exposed' to root node
    for p in policies :
        if not p['enabled'] :
            continue

        resources = flatten_resources(p['id'],p['tenant'],p['num_id'],p['resources'],resource_classes)
        graph.add_node(p['id'],path_node = False, num_id = p['num_id'],exposed=p['exposed'],
                   resources = resources,inherited = list())

        #if p['exposed'] :   # if exposed == False then it cannot be assigned directly to user
        # connect all nodes to "root"
        graph.add_edge('root',p['id'],type='root_policy')

    # Add edges from inherited analysis
    for p in policies :
        for pih in p['inheritedResources'] :
            graph.nodes[p['id']]['path_node'] = True
            graph.nodes[pih['policyId']]['path_node'] = True
            graph.add_edge(pih['policyId'],p['id'],type ='policy_policy')

    return graph

# recursively acrue resources
def get_inherited_resources(graph,node) :
    pn = [n for n in graph.predecessors(node) if not n == 'root']
    for pn_node in pn :
        inh_r = get_inherited_resources(graph,pn_node)
        for r in inh_r :
            r['inheritPath'] = r['inheritPath']+':'+r['policyId'] if 'inheritPath' in r else r['policyId']
            r['policyId'] = node
            r['inheritedFrom'] = pn_node
        graph.nodes[node]["inherited"].extend(inh_r)
    new_inh_resources = deepcopy(graph.nodes[node]["resources"])
    new_inh_resources.extend(deepcopy(graph.nodes[node]["inherited"]))
    return  new_inh_resources

def flatten_resources(policy_id,tenant,num_id,policy_resources,resource_classes) :
    resources = list()
    for r in policy_resources :
        resource = { k: v for k,v in r.items() if not isinstance(v,dict)}
        resource.update(r['contentData'])
        resource['tenant'] = tenant
        resource['policyId'] = policy_id
        #resource['numId'] = num_id
        resource['resourceClass'] = resource_classes[resource['resourceType']]
        resources.append(resource)
    return resources

# add inherited resources to policy resources
def add_inherited_resources(graph) :
    logging.info("Add resources to graph and accrue inherited resources")
    resources = list()
    for node in graph.nodes :
        resources.extend(get_inherited_resources(graph,node))
    return resources


# save acrued and flattened
def save_resources(filename, resources) :
    '''
    Save the resources including the inherited ones to file as csv
    :param filename:  filename with path to save resources
    :param resources: list of resource dictionariers
    :return: None
    '''

    logging.info("Save resources to \"resources.csv\"")

    # Test if new resource attributes had been found
    resource_attributes = {s  for r in resources for s in r.keys()}
    header = ('tenant','policyId','num_id','level','policyClass','exposed','pathNode','inheritPath','inheritedFrom','duplicatesYN','resourceClass','resourceType','activity','name','connectionId','canStopForUser','technicalName')
    if not len(resource_attributes-set(header)) == 0 :
        logging.warning(f"More resource attributes than expected: {resource_attributes-header}")

    # writing resource after sorting it for a convenient usage
    with open(filename, 'w') as csv_file:
        fwriter = csv.writer(csv_file)
        fwriter.writerow(header)
        for r in resources:
            row = list()
            for h in header :
                row.append(r[h] if h in r else '')
            fwriter.writerow(row)


def check_duplicate_resources(resources) :
    logging.info("Check for duplicates")
    df = pd.DataFrame(resources).fillna('')
    check_on = ('tenant','policyId','resourceType','activity','name','connectionId','canStopForUser','technicalName')
    check_on = [ col for col in df.columns if col in check_on]
    df['duplicatesYN'] = df.duplicated(subset=check_on,keep=False).replace({True:'Y',False:'N'})
    logging.info(f"Number of duplicate resources: {df.loc[df['duplicatesYN']== 'Y'].shape[0]}")
    return df.to_dict('records')

def classify_policy(graph,resources,threshold_diff = 1,threshold_ratio = 0.9) :
    logging.info("Classify analysis")
    df = pd.DataFrame(resources).fillna('')
    rc_df = df.groupby(['policyId','resourceClass'])['name'].count().reset_index()
    rc_df = rc_df.rename(columns={'name':'count'})
    rc_df['sum'] = rc_df['count'].groupby(rc_df['policyId']).transform('sum')
    rc_df['ratio'] =rc_df['count']/rc_df['sum']
    rc_df = rc_df.groupby('policyId').max()
    rc_df['diff'] =rc_df['sum']- rc_df['count']
    rc_df['policyClass'] = 'multiple'
    rc_df.loc[(rc_df['diff']<=threshold_diff) | (rc_df['ratio']>threshold_ratio),'policyClass'] = rc_df.loc[(rc_df['diff']<=1) | (rc_df['ratio']>0.9),'resourceClass']
    for n in graph.nodes :
        if n == 'root':
            continue
        graph.nodes[n]['policyClass'] = rc_df.loc[n]['policyClass']
    rc_df.reset_index(inplace=True)
    rc_df.drop(columns=['count','ratio','diff','sum','resourceClass'],inplace=True)
    df = pd.merge(df,rc_df,how='left',on='policyId')
    return df.to_dict('records')

def add_path_node_info(graph,resources) :
    logging.info("Add path_node info")
    df = pd.DataFrame(resources).fillna('')
    df['pathNode'] = ''
    policy_path_nodes = [n for n,att in graph.nodes(data=True) if att['path_node']==True]
    df.loc[df['policyId'].isin(policy_path_nodes),'pathNode'] = 'Y'
    return df.to_dict('records')

def add_node_att(graph,resources) :
    logging.info("Add level")
    df = pd.DataFrame(resources).fillna('')
    nodes_level = [ (n,att['num_id'],att['level'],att['exposed']) for n, att in graph.nodes(data=True)]
    nl_df = pd.DataFrame.from_records(nodes_level,columns=['policyId','num_id','level','exposed'])
    df = pd.merge(df,nl_df,on='policyId',how='inner')
    return df.to_dict('records')

def run_policy_network(filename) :

    with open(filename, 'r') as json_file:
        policies = json.load(json_file)

    G = build_network(policies)

    draw_graph(G)



############### MAIN #######################

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    with open('../../config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    with open('../../policies/policies.json', 'r') as json_file:
        policies = json.load(json_file)

    G = build_network(policies,params['RESOURCE_CLASSES'])

    tag_nodes_by_name(G,'mycompany','mycompany',successor_nodes=True,remove_untagged=True)
    fn = filter_nodes(G,att_filter={'mycompany':True})
    for n in G.nodes:
        print(n)



