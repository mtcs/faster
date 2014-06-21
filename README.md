Faster
======
####Super Fast Distributed Computing

Faster is a distributed computing framework designed to be fast, efficient and flexible. It is designed to work well in small and large heterogeneous clusters. Although, we are still in heavy development.


__Planned Features:__

* High level distributed computing framework.
* Implemented in C++ for speed. 
* Optimized for heterogeneous clusters with a dynamic balancing of data blocks.
* Functional ideology similar to Apache Spark(map, reduce, flatMap, groupByKey, coGroup etc).
* Built-in bulk functions (bulkMap, bulkReduce etc).
* OpenMP parallelization.
* MPI message passing (?).
* Easy to use with Cuda/OpenCL.


Implementation Progress:
-----------------------

__Done:__

* Basic FDD templates (single item per line and no dynamic balancing).
* Distributed functions: map, bulkMap, reduce and bulkReduce.
* Process handling using MPI.
* MPI communication interface.

__TODO list:__

* FlatMap function
* Block traking
* Array FDD template
* Pair FDD template
* Block balancing
* ...
* FDD Fault tolerance



How it works
------------

Faster works on distributed datasets called FDDs. These datasets are (or will be) fault tolerant data storages responsible to store data and handle distributed functions. Those functions can be predefined or customized by the user. Next we provide a example of a simple program that uses Faster libraries;

__1. Initialize a context and start workers:__

```cpp
fastContext fc();
``` 

__2. Register your custom functions:__
	
```cpp
fc.registerFunction((void*) &map1);
fc.registerFunction((void*) &reduce1);
```

__3. Start workers:__

```cpp
fc.startWorkers();
```
	
Every program must have at least one worker 

__WARNING: in MPI mode, the code before this call is executed by all processes and the code after is NOT.__

__4. Create a FDD dataset. In this case we will use a int array already in memory:__

```cpp
fdd <int> data(fc, data, [NUMBER_OF_ITEMS]);
```

__5. Apply your functions to your data and get the result:__

```cpp
int result = data.map<int>((void*) &map1)->reduce((void*) &reduce1)/NUMITEMS;
```

A full example would be:

```cpp
#include <iostream>
#include "libfaster.h"

#define NUMITEMS 100*1000

using namespace std;

int map1(int & input){
	return input * 2;
}


int reduce1(int &a, int &b){
	return a + b;
}


int main(int argc, char ** argv){
	cout << "Init FastLib" << '\n';
	fastContext fc("local");

	fc.registerFunction((void*) &map1);
	fc.registerFunction((void*) &reduce1);

	fc.startWorkers();

	cout << "Generate Data" << '\n';
	int rawdata[NUMITEMS];

	for ( int i = 0; i < NUMITEMS; ++i )
		rawdata[i] = 1;

	cout << "Import Data" << '\n';
	fdd <int> data(fc, rawdata, NUMITEMS);

	cout << "Process Data" << '\n';
	int result = data.map<int>((void*) &map1)->reduce((void*) &reduce1)/NUMITEMS;
		
	cout << "DONE!" << '\n';
	std::cout << result << "\n";
	
	return 0;
}
```



