#!/bin/bash
mpirun -np 3 xterm -geometry 110x30 -e gdb -ex=r ./examples/fexample-int
#mpirun -np 3 xterm -geometry 120x30 -e "valgrind --max-stackframe ./examples/fexample-int ;sleep 1000"
