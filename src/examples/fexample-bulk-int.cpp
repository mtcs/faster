#include <iostream>
#include <stdlib.h>
#include <omp.h>
#include "libfaster.h"

#define NUMITEMS 100*1000

using namespace std;

void bulkMap1(int * output, int * input, size_t size){
	#pragma omp parallel for 
	for (int i = 0 ; i < size ; ++i){
		output[i] = input[i] * 2;
	}

}


int bulkReduce1(int * input, size_t size){
	int result = 0;
	
	#pragma omp parallel 
	{
		int nT = omp_get_num_threads();
		int tN = omp_get_thread_num();
		int partResult = 0;

		#pragma omp for 
		for (int i = 0; i < size; ++i){
			partResult += input[i];
		}
		
		#pragma omp atomic 
		result += partResult;
	}

	return result;
}

int main(int argc, char ** argv){
	// Init Faster Framework
	cout << "Init FastLib" << '\n';
	fastContext fc("local");
	fc.registerFunction((void*)&bulkMap1);
	fc.registerFunction((void*)&bulkReduce1);

	fc.startWorkers();

	cout << "Generate Data" << '\n';
	int rawdata[NUMITEMS];

	for ( int i = 0; i < NUMITEMS; ++i )
		rawdata[i] = 1;

	cout << "Import Data" << '\n';
	fdd <int> data(fc, rawdata, NUMITEMS);

	cout << "Process Data" << '\n';
	int result = data.bulkMap<int>((void*)&bulkMap1)->bulkReduce((void*)&bulkReduce1);

	cout << "DONE!" << '\n';
	std::cout << result << "\n";

	return EXIT_SUCCESS;
}
