#!/bin/bash
mpirun -np 3 xterm -fn "*-fixed-*-*-*-13-*" -geometry 90x33 -e gdb -ex=r ./examples/fexample-int
#mpirun -np 3 xterm -geometry 120x30 -e "valgrind --max-stackframe ./examples/fexample-int ;sleep 1000"
