#!/usr/bin/python3

import sys
import functools

if ( len(sys.argv) < 2 ) :
    sys.exit("ERROR: usage: #pagerank.py <graph file>(string) [error(float)]\n")

gfile = open ( sys.argv[1], 'r')
resolution = 1 if (len(sys.argv) < 3) else float(sys.argv[2])
dumping = 0.85 if (len(sys.argv) < 4) else float(sys.argv[3])
verbose = 0 if (len(sys.argv) < 5) else int(sys.argv[4])

G = {}
numNodes = 0

for line in gfile :
    tokens = line.split()
    node = int(tokens[0])
    G[node] = list(map(int, tokens[1:]))
    numNodes = functools.reduce(max, G[node], numNodes)

numNodes = numNodes + 1
print ( numNodes, " node graph", file = sys.stderr)
gfile.close()

error = 10;
pr = {}
newpr = {}

for node, neighbs in G.items(): 
    pr[node] = 1.0 / numNodes;

iteration = 0
while ( iteration < 10) :
    print ( "Iteration:", iteration, file = sys.stderr, end = " ")
    iteration += 1

    for node in G.keys(): 
        newpr[node] = (1 - dumping) / numNodes

    # Give Pagerank
    for node, neighbs in G.items(): 
        if ( verbose > 1 ) :
            print("[{}:".format(node),file = sys.stderr, end = '')
        for neighb in neighbs: 
            if neighb not in newpr :
                newpr[neighb] =  (1 - dumping) / numNodes
            contrib = dumping * pr[node] / len(neighbs)
            if ( verbose > 1 ) :
                print ( "{}>{} ".format(contrib, neighb), file = sys.stderr, end = '')
            newpr[neighb] += contrib 
        if ( verbose > 1 ) :
            print("]",file = sys.stderr)

    error = 0;
    for node in G.keys(): 
        # Calulate error
        error = max(error, abs(newpr[node] - pr[node]))

    print ( "Error: %9.5f" % error, file = sys.stderr)

    newpr, pr = pr, newpr

    if ( verbose ) :
        for node in sorted(G.keys()): 
            #print ( "{}:{:.5f}  ".format(node, pr[node]), file = sys.stderr, end = '')
            print ( "{} {:.5f}  ".format(node, pr[node]), file = sys.stderr, end = '')
            #print ( "{:.2f}".format(pr[node]), file = sys.stderr , end = ' ')


for node in sorted(G.keys()): 
    print ("{} {:.16f}".format(node, pr[node]))
