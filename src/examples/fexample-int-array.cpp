#include <iostream>
#include "libfaster.h"

#define NUMITEMS 10*1000

using namespace std;

// Multiply every array element by 2
void map1(int *& output, size_t & rSize, int * input, size_t size){
	rSize = size;
	output = new int[rSize];

	//multiply the entire vector by 2
	for ( int i = 0; i < size; ++i ){
		output[i] = input[i] / 2;
	}

}

// Sums the values in the respective positions of A and B
void reduce1(int *& out, size_t & oSize, int *a, size_t sizeA, int * b, size_t sizeB){
	oSize = max(sizeA, sizeB);
	out = new int[oSize];

	for ( int i = 0; i < oSize; ++i ){
		if ( i < sizeA){
			if( i < sizeB){
				out[i] = a[i] + b[i];
			}else{
				out[i] = a[i];
			}
		}else{
			out[i] = b[i];
		}
	}
}


int main(int argc, char ** argv){
	// Init Faster Framework
	cout << "Init FastLib" << '\n';
	fastContext fc("local");

	fc.registerFunction((void*) &map1);
	fc.registerFunction((void*) &reduce1);

	fc.startWorkers();

	cout << "Generate Data" << '\n';
	int * rawdata[NUMITEMS];
	size_t dataSizes[NUMITEMS];

	// Create a random adjacency matrix
	for ( size_t i = 0; i < NUMITEMS; ++i ){
		
		// Random number of items
		dataSizes[i] = rand() % 10;

		rawdata[i] = new int[dataSizes[i]];

		for ( size_t j = 0; j < (dataSizes[i]); ++j )
			// edge connected to a random vertex
			rawdata[i][j] = 2;
	}

	cout << "Import Data" << '\n';
	fdd <int * > data(fc, rawdata, dataSizes, NUMITEMS);

	cout << "Process Data" << '\n';
	vector<int> result = data.map<int *>(&map1)->reduce(&reduce1);

	for ( int i = 0; i < result.size(); ++i){
		cout << result[i] << ' ';
	}
	cout << '\n';


	cout << "DONE!" << '\n';
	std::cout << result[0] << "\n";

	for ( int i = 0; i < NUMITEMS; ++i){
		delete [] rawdata[i];
	}

	return 0;
}
