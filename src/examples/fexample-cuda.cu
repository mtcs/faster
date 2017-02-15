#include <iostream>
#include "libfaster.h"

#define NUMITEMS 10*1000

using namespace std;
using namespace faster;

typedef int (* map1_t)(int &);

__device__ int map1(int & input){
	return input / 2;
}

int reduce1(int &a, int &b){
	return a + b;
}



int main(int argc, char ** argv){
	// Init Faster Framework
	cout << "Init FastLib" << '\n';
	fastContext fc(argc,argv);

	__device__ map1_t map1_d = & map1;
	map1_t map1_h;
	cudaMemcpyFromSymbol(&map1_h, map1_d, sizeof(pointFunction_t));
	fc.registerFunction((void*) &map1_h);
	fc.registerFunction((void*) &reduce1);

	fc.startWorkers();
	if (!fc.isDriver())
		return 0;

	cout << "Generate Data" << '\n';
	int rawdata[NUMITEMS];

	for ( int i = 0; i < NUMITEMS; ++i )
		rawdata[i] = 2;

	cout << "Import Data" << '\n';
	fdd <int> data(fc, rawdata, NUMITEMS);

	cout << "Process Data" << '\n';
	int result = data.map<int>(&map1_h)->reduce(&reduce1);

	cout << "DONE!" << '\n';

	std::cout << "Resut:" << result << "\n";

	return 0;
}
