#!/bin/bash
mpirun -np 3 xterm -fn "*-fixed-*-*-*-13-*" -geometry 90x33 -e gdb -ex=r ./examples/fexample-int
#mpirun -np 3 xterm -fn "*-fixed-*-*-*-13-*" -geometry 90x33 -e "valgrind --max-stackframe ./examples/fexample-int ;sleep 1000"
#mpirun -np 3 xterm -e "valgrind --max-stackframe ./examples/fexample-int &>log \$OMPI_COMM_WORLD_RANK"
