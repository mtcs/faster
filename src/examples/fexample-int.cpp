#include <iostream>
#include <stdlib.h>
#include <omp.h>
#include "libfaster.h"

#define NUMITEMS 100*1000

using namespace std;

int map1(int & input){
	return input / 2;
}

void bulkMap1(int * output, int * input, size_t size){
	#pragma omp parallel for 
	for (int i = 0 ; i < size ; ++i){
		output[i] = input[i] / 2;
	}

}

int reduce1(int &a, int &b){
	return a + b;
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

void * funcTable [] = {
	(void *) &map1, 	// 0
	(void *) &reduce1	// 1
};

int main(int argc, char ** argv){
	// Init Faster Framework
	cout << "Init FastLib" << '\n';
	fastContext fc("local", funcTable);

	cout << "Generate Data" << '\n';
	int rawdata[NUMITEMS];

	for ( int i = 0; i < NUMITEMS; ++i )
		rawdata[i] = i+1;

	cout << "Import Data" << '\n';
	fdd <int> data(fc, rawdata, NUMITEMS);

	cout << "Process Data" << '\n';
	int result = data.map<int>(0)->reduce(1);

	cout << "DONE!" << '\n';
	std::cout << result << "\n";

	return EXIT_SUCCESS;
}
