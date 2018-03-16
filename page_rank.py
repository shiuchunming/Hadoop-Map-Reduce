#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 13 21:49:04 2017

@author: alpha
"""

#pr_file = './input/twitter_dist_1.txt'
pr_file = './input/basic.txt'
#pr_file = './input/toy.txt'


twitter_input = open(pr_file,'r')

adj_list = {}
in_list = {}
dup_list = {}

for line in twitter_input:
    val = line.split()
    if val[0] not in adj_list:
        adj_list[val[0]] = [val[1]]
    else:
        adj_list[val[0]].append(val[1])
    if val[1] not in adj_list:
        adj_list[val[1]] = []
    
    if val[1] not in in_list:
        in_list[val[1]] = [val[0]]
    else:
        in_list[val[1]].append(val[0])
    if val[0] not in in_list:
        in_list[val[0]] = []
        

random_jump = 0.2
num_of_node = len(adj_list.keys())
iteration = 1

page_rank = {}

for node in adj_list.keys():
    page_rank[node] = 1/num_of_node
    
page_rank_result = page_rank
    

for i in range(iteration):
    page_rank = page_rank_result
    page_rank_result = {}
    for node, in_nodes in in_list.items():
        res = random_jump*(1/num_of_node)
        sum_in = 0
        for in_node in in_nodes:
            sum_in += page_rank[in_node]/len(adj_list[in_node])
        res += (1-random_jump)*(sum_in)
        
        page_rank_result[node] = res
        
        


import networkx as nx

G = nx.Graph()
G.add_nodes_from(adj_list.keys())
for node, edges in adj_list.items():
    for edge in edges:
        G.add_edge(node,edge)
        
pr = nx.pagerank(G, alpha=0.9,max_iter = 2)

