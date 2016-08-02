![Faster](https://github.com/mtcs/faster/wiki/img/Logo.png)
======
####Super Fast Distributed Computing

http://mtcs.github.io/faster

Faster is a distributed computing framework designed to be fast, efficient and flexible. It is designed to work well in small and large heterogeneous clusters. Although, we are still in heavy development.

|unix_build|

__Features:__

* High level distributed computing framework.
* Implemented in C++ for speed.
* Very low latency optimized for iterative and high performance algorithms.
* Obect-Functional ideology similar to Apache Spark(map, reduce, flatMap, groupByKey, cogroup etc).
* Built-in bulk functions (bulkMap, bulkReduce etc).
* Minimal memmory usage.
* OpenMP parallelization.
* MPI message passing wrapping.
* Easy to use with OpenMP GPU device accelerators.


.. |unix_build| image:: https://travis-ci.org/mtcs/faster.svg?branch=dev%2Fmaster
	:target: https://travis-ci.org/mtcs/faster
	:alt: Build Status


