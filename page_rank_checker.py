#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from math import sqrt


#file1 = './result/hello.txt'
file1 = './result/pagerank_our_group.txt'
file2 = './result/twitter2_2_0.2_0.txt'

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
        res[val[0]] = float(val[1])
    else:
        dup[val[0]] = float(val[1])
        

for line in r2:
    val = line.split()
    if val[0] in res:
        if abs(float(val[1]) - res[val[0]]) < 0.00000001:
            equ[val[0]] = float(val[1])
        else:
            diff[val[0]] = float(val[1])
    else:
        not_exist[val[0]] = float(val[1])
        
        
        
        
print('num of equ: %i', len(equ.keys()))
print('num of diff: %i', len(diff.keys()))
print('num of diff: %i', len(not_exist.keys()))

