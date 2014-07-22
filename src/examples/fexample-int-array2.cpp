#include <iostream>
#include "libfaster.h"

#define NUMITEMS 10*1000

using namespace std;
using namespace faster;

// Remove duplicated links
int * map1(int * input, size_t size){
	bool present [NUMITEMS];
	int outputSize = size;
	int * output;

	memset(present, 0, sizeof(bool) * NUMITEMS );

	// Detect duplicated vertices
	for ( int i = 1; i < size ; ++i){
		if ( present[input[i]] ){
			outputSize --;
		}else{
			present[input[i]] = true;
		}
	}

	memset(present, 0, sizeof(bool) * NUMITEMS );

	output = new int[outputSize];
	output[0] = input[0];

	int outputPos = 1;

	// Insert unique vertices
	for ( int i = 1; i < size ; ++i){
		if ( ! present[input[i]] ){
			present[input[i]] = true;
			output[outputPos++] = input[i];
		}
	}

	return output;
}

// Returns the vertex with connections to more higher index vertices (not very useful, I know...)
void reduce1(int *& out, size_t & oSize, int *a, size_t sizeA, int *b, size_t sizeB){
	unsigned int sumA = 0, sumB = 0;

	for ( int i = 1; i < sizeA ; ++i){
		sumA += a[i];
	}

	for ( int i = 1; i < sizeB ; ++i){
		sumB += b[i];
	}

	if (sumA > sumB){
		out = new int[sizeA];
		memcpy(out, a, sizeA);
		oSize = sizeA;
	}else{
		out = new int[sizeB];
		memcpy(out, b, sizeB);
		oSize = sizeB;
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
		rawdata[i] = new int[i];
		
		// Random number of edges
		dataSizes[i] = rand() % 100;

		// First array position contains the vertex index
		rawdata[i][0] = i;

		for ( size_t j = 1; j < (dataSizes[i] + 1); ++j )
			// edge connected to a random vertex
			rawdata[i][j] = rand() % NUMITEMS;
	}

	cout << "Import Data" << '\n';
	fdd <int * > data(fc, rawdata, dataSizes, NUMITEMS);

	cout << "Process Data" << '\n';
	int * result;
	size_t rSize;
	data.map<int *>(&map1)->reduce(result, rSize, &reduce1);

	for ( int i = 0; i < rSize; ++i){
		cout << result[i] << ' ';
	}
	cout << '\n';

	cout << "DONE!" << '\n';
	std::cout << result[0] << "\n";

	return 0;
}
