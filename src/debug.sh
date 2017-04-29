#!/bin/bash
#export LD_PRELOAD=libasan.so.3:liblsan.so.0
#export ASAN_OPTIONS=symbolize=1:abort_on_error=0:verbosity=1
#export ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer

#mpirun -np 4 xterm -fn "*-fixed-*-*-*-12-*" -geometry 90x37 -e gdb -ex="r 10000 5" ./test/latency
mpirun -np 4 xterm -fn "*-fixed-*-*-*-12-*" -geometry 90x37 -e valgrind ./test/latency 10000 5
#mpirun -np 4 xterm -fn "*-fixed-*-*-*-13-*" -geometry 90x33 -e "valgrind --max-stackframe ./examples/fexample-int ;sleep 1000"
#mpirun -np 12 xterm -fn "*-fixed-*-*-*-8-*" -geometry 84x28 -e gdb -ex="r ../res/graph1000.al > /dev/null" ./examples/pagerank
#mpirun -np 4 xterm -e "valgrind --max-stackframe=8000000 ./examples/fexample-int &>log\$OMPI_COMM_WORLD_RANK"
