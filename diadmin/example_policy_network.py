
import json
import networkx as nx
import matplotlib.pyplot as plt

# read policy details file
with open("policy_list.json", 'r') as json_file:
    policies = json.load(json_file)

g = nx.DiGraph()

################ NETWORK ############################
# Hierarchical graph needs a root graph
g.add_node('root',tenant=policies[0]['tenant'],path_node=True,num_id = 0,exposed=False)

# Add all policies as node and connect 'exposed' to root node
for p in policies :
    if not p['enabled'] :
        continue

    g.add_node(p['id'],path_node = False, num_id = p['num_id'],exposed=p['exposed'] )

    if p['exposed'] :   # if exposed == False then it cannot be assigned directly to user
        g.add_edge('root',p['id'],type='root_policy')

# Add edges from inherited policies
for p in policies :
    for pih in p['inheritedResources'] :
        g.nodes[p['id']]['path_node'] = True
        g.nodes[pih['policyId']]['path_node'] = True
        g.add_edge(pih['policyId'],p['id'],type ='policy_policy')

################ NODE LEVEL ############################
# For each node calculate the max distance to the root node
# For the graph get the max distance
max_graph_distance = 2
for n,att in g.nodes(data=True) :
    spaths = nx.all_simple_paths(g,'root',n)
    maxs = 2
    for s in spaths :
        if len(s) > maxs :
            maxs = len(s)
    g.nodes[n]['max_distance'] = maxs -1
    if maxs > max_graph_distance :
        max_graph_distance = maxs
    g.nodes['root']['max_graph_distance'] = max_graph_distance

################ DRAW ############################
# position of each node that is part of a longer path in the chart (height: 1, width: 1)
# policies that are not sharing their resources are omitted
pos = {'root':(0.5,1.)}
policy_path_nodes = [n for n,att in g.nodes(data=True) if att['path_node']==True]
dx = 1.0/len(policy_path_nodes)
dy = 1.0/g.nodes['root']['max_graph_distance']
x = 0
i_1 = -1.
for node in policy_path_nodes :
    i_1 = -1. if i_1 == 1 else 1.
    if node == 'root' :
        continue
    y = 1.0 - (dy * g.nodes[node]['max_distance']) + (0.03 *  i_1)
    x += dx
    pos[node]=(x,y)

# Draw
policy_path_edges = [(f,t) for f,t,att in g.edges(data=True) if att['type'] =='policy_policy']
root_path_edges = [(f,t) for f,t,att in g.edges(data=True) if att['type'] =='root_policy' and g.nodes[t]['path_node'] == True]

#labels
labels = { p: g.nodes[p]['num_id'] for p in list(policy_path_nodes)}
nx.draw_networkx_labels(g,pos,labels,font_size=8,font_color='w')

#nodes
options = {"edgecolors": "tab:gray", "node_size": 100, "alpha": 0.9}
nx.draw_networkx_nodes(g, pos, policy_path_nodes, node_color="dimgrey", **options)

#edges
nx.draw_networkx_edges(g,pos,policy_path_edges, width=0.1,edge_color='blue')
nx.draw_networkx_edges(g,pos,root_path_edges, width=0.1,edge_color='red')

plt.show()
