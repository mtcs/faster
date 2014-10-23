#!/usr/bin/python3

import sys

if ( len(sys.argv) < 2 ) :
    sys.exit("ERROR: usage: #pagerank.py graph file(string) [error(float)]\n")

g = open ( sys.argv[1], 'r')
resolution = 1 if (len(sys.argv) < 3) else float(sys.argv[2])
dumping = 0.85 if (len(sys.argv) < 4) else float(sys.argv[3])

G = {}

for line in g :
    tokens = line.split()
    node = int(tokens[0])
    G[node] = list(map(int, tokens[1:]))

g.close()

error = 10;
pr = {}
newpr = {}

for node, neighbs in G.items(): 
    pr[node] = 1.0;

iteration = 0
while ( error > resolution) :
    print ( "Iteration:", iteration, file = sys.stderr, end = " ")
    iteration += 1

    for node in G.keys(): 
        newpr[node] = (1 - dumping)

    # Give Pagerank
    for node, neighbs in G.items(): 
        for neighb in neighbs: 
            newpr[neighb] += dumping * pr[node] / len(neighbs);

    error = 0;
    for node in G.keys(): 
        # Calulate error
        error = max(error, abs(newpr[node] - pr[node]))

    print ( "Error: %9.5f" % error, file = sys.stderr)

    newpr, pr = pr, newpr

for node in sorted(G.keys()): 
    print ("{} {:.8f}".format(node, pr[node]))
