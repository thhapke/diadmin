
from subprocess import check_output, run
import logging
import csv
from pprint import pprint
import json
import sys
import random

import yaml
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors


from login import di_login

#def get_policies(format = 'json') :
#    policies_cmd = ['vctl','policy','policies','--format',format]
#    policies = check_output(policies_cmd).decode('utf-8')
#    return policies


def get_policy(policy_id) :
    policies_cmd = ['vctl','policy','policy',policy_id]
    policy = check_output(policies_cmd).decode('utf-8')
    return policy

def get_list_policies(format):
    list_policies_cmd = ['vctl','policy','list-policies','--format',format]
    list_policies = check_output(list_policies_cmd).decode('utf-8')
    return list_policies

def get_policy_get(policy_id,format) :
    policies_cmd = ['vctl','policy','get',policy_id,'--format',format]
    policy = check_output(policies_cmd).decode('utf-8')
    return policy


def get_policy_list_assignments(user=None,format='json') :
    if not user :
        assignements_cmd = ['vctl','policy','list-assignments','-a','--format',format]
    else :
        assignements_cmd = ['vctl','policy','list-assignments',user,'--format',format]
    assignements = check_output(assignements_cmd).decode('utf-8')
    return assignements


def get_policy_resources() :
    resources_cmd = ['vctl','policy','resources']
    res = check_output(resources_cmd).decode('utf-8').split('\n')
    resources = list()
    result = list()
    for r in  res :
        rec = r.split()
        if len(rec) and not rec[0] == 'ID' and not rec[1] == 'Visibility' :
            result.append({'ID':rec[0],'Visibility':rec[1]})
            resources.append(rec[0])
    logging.info(f'Number of Resources: {len(resources)}')
    return result, resources


def get_all_policies() :
    '''
    Getting all policies with details using vctl
    :return: None
    '''

    list_policies = get_list_policies(format='json')
    policies = json.loads(list_policies)

    pd_list = list()
    for p in policies :
        logging.info(f"Get details: {p['id']}")
        pd_list.append(json.loads(get_policy_get(p['id'],format='json')))

    # add integer id to each policy for easing later analysis

    return pd_list

# depracted
# id is created when downloading the policy details
num_id = -1
def gen_id() :
    global num_id
    num_id += 1
    return num_id

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

def print_nodes(graph,att_filter=None,node_filter=None) :
    print("=== NODES ===")
    num_passed_items = 0
    for node,att in graph.nodes(data=True) :
        passed = True
        if att_filter :
            for f,v in att_filter.items() :
                if att[f] != v :
                    passed = False
                    break
        if node_filter :
            if not (node in node_filter ) :
                passed = False
        if passed :
            num_passed_items += 1
            print(f"{graph.nodes[node]['num_id']}/{node}: {att} ")
    logging.info(f"Filtered: {num_passed_items}/{len(graph.nodes)}")

def print_node_connection(graph,filter,nodes=None):
    print("=== NODE CONNECTIONS ===")
    if not nodes :
        nodes = G.nodes
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

# longest path from root to node
def add_max_distance(G):
    '''
    Calculates the maximum direct path to root
    :param G: networkx directed graph
    :return: None
    '''
    max_graph_distance = 2
    for n,att in G.nodes(data=True) :
        spaths = nx.all_simple_paths(G,'root',n)
        maxs = 2
        for s in spaths :
            if len(s) > maxs :
                maxs = len(s)
        G.nodes[n]['max_distance'] = maxs -1
        if maxs > max_graph_distance :
            max_graph_distance = maxs
    G.nodes['root']['max_graph_distance'] = max_graph_distance

# Calculates the position of the nodes
def calc_pos(G, nodes, width=1., height = 1.0,y_width_band = 0.03 ):
    '''
    Calculates the position of the nodes in a box (1.,1)
    :param G: networkx graph
    :param nodes: list of nodes
    :param width: plot width
    :param height: plot height
    :param y_width_band: scatters nodes within this band
    :return: node positions
    '''
    dx = width/len(nodes)
    dy = height/G.nodes['root']['max_graph_distance']
    ddy = y_width_band * height
    x = 0
    pos = {'root':(width*0.5,1.)}
    i_1 = -1.
    for node in nodes :
        i_1 = -1. if i_1 == 1 else 1.
        if node == 'root' :
            continue
        y = 1.0 - (dy * G.nodes[node]['max_distance']) + (ddy *  i_1)
        x += dx
        pos[node]=(x,y)
    return pos

# Draws network with distance levels
def draw_graph(graph) :
    '''
    Draws network with horizontal levels with a additional 'root'-node
    :param graph: network graph
    :return: None
    '''

    # filter nodes and edges
    policy_path_nodes = [n for n,att in graph.nodes(data=True) if att['path_node']==True]
    policy_path_edges = [(f,t) for f,t,att in graph.edges(data=True) if att['type'] =='policy_policy']
    root_path_edges = [(f,t) for f,t,att in graph.edges(data=True) if att['type'] =='root_policy' and graph.nodes[t]['path_node'] == True]

    pos = calc_pos(graph,policy_path_nodes)

    #labels
    labels = { p: graph.nodes[p]['num_id'] for p in list(policy_path_nodes)}
    nx.draw_networkx_labels(graph,pos,labels,font_size=8,font_color='w')

    #nodes
    options = {"edgecolors": "tab:gray", "node_size": 100, "alpha": 0.9}
    nx.draw_networkx_nodes(graph, pos, policy_path_nodes, node_color="dimgrey", **options)

    #edges
    nx.draw_networkx_edges(graph,pos,policy_path_edges, width=0.1,edge_color='blue')
    nx.draw_networkx_edges(graph,pos,root_path_edges, width=0.1,edge_color='red')

    plt.show()


# network of policies
def build_network(policies) :
    '''
    Build network form policies with a root node that connects to all 'exposed' nodes. In addition edges
    connect policy nodes that inherit from policy nodes
    :param policies: List of policies created by calling 'vctl policy get <id>' interatively
    :return: networkx graph
    '''
    g = nx.DiGraph()

    # Hierarchical graph needs a root graph
    g.add_node('root',tenant=policies[0]['tenant'],path_node=True,num_id = gen_id(),exposed=False)

    # Add all policies as node and connect 'exposed' to root node
    for p in policies :
        if not p['enabled'] :
            continue

        g.add_node(p['id'],path_node = False, num_id = gen_id(),exposed=p['exposed'] )

        if p['exposed'] :   # if exposed == False then it cannot be assigned directly to user
            g.add_edge('root',p['id'],type='root_policy')

    # Add edges from inherited policies
    for p in policies :
        for pih in p['inheritedResources'] :
            if g.has_node(pih['policyId']) :
                g.nodes[p['id']]['path_node'] = True
                g.nodes[pih['policyId']]['path_node'] = True
                g.add_edge(pih['policyId'],p['id'],type ='policy_policy')
            else :
                logging.warning(f"{p[id]} - Inherited Node not in Graph: {pih[id]})")

    return g

def create_resources_list(policies) :
    resources = list()
    for p in policies :
        for r in p['resources'] :
            con_id = r['contentData']['connectionId'] if "connectionId" in r['contentData'] else ''
            resource = {'policy_id':p,
                        'resource_type':r['resourceType'],
                        'activity':r['contentData']['activity'],
                        'connection_id':con_id}
            resources.append(resource)
    return resources

def run_policy_network() :
    filename = '../data/policy_details.json'
    #policy_details = get_all_policies()
    #with open(filename, 'w') as json_file:
    #    json.dump(policy_details,json_file,indent=4)

    with open(filename, 'r') as json_file:
        policies = json.load(json_file)
    if len(policies) == 0 :
        raise ValueError("<policies> list has no members!")

    G = build_network(policies)
    add_max_distance(G)

    #print_node_connection(G,filter={'path_node':True})
    #print_edges(G,filter={'type':'root_policy'})
    #print_edges(G,att_filter={},node_filter=['root'])

    draw_graph(G)


############### MAIN #######################

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    with open('../config.yaml') as yamls:
        params = yaml.safe_load(yamls)

    #### LOGIN
    di_login(params)
    
    run_policy_network()

    #result, resources = get_policy_resources()
    #pprint(resources)



