#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#file1 = './result/hello.txt'
file1 = './result/twitter_1_iteration_0.txt'
file2 = './result/output0.txt'

r1 = open(file1,'r')
r1lines = []
r2 = open(file2,'r')
r2lines = []

res = {}
dup = {}
diff = {}
equ = {}
not_exist = {}

for line in r1:
    val = line.split()
    if val[0] not in res:
        res[val[0]] = val[1]
    else:
        dup[val[0]] = val[1]
        

for line in r2:
    val = line.split()
    if val[0] in res:
        if val[1] == res[val[0]]:
            equ[val[0]] = val[1]
        else:
            diff[val[0]] = val[1]
    else:
        not_exist[val[0]] = val[1]
        
        
        
        
print('num of equ: %i', len(equ.keys()))
print('num of diff: %i', len(diff.keys()))
print('num of diff: %i', len(not_exist.keys()))


from dijkstra_alg import Graph
from dijkstra_alg import dijsktra

