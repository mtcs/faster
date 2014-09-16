#include <iostream>
#include <vector>
#include "libfaster.h"

#define NUMITEMS 10*1000

using namespace std;
using namespace faster;

// Multiply every array element by 2
vector<int> map1(vector<int> & input){
	vector<int> output(input.size());

	//multiply the entire vector by 2
	for ( size_t i = 0; i < input.size(); ++i ){
		output[i] = input[i] / 2;
	}

	return output;

}

// Sums the values in the respective positions of A and B
vector<int>  reduce1(vector<int> & a, vector<int> & b){
	size_t oSize = max(a.size(), b.size());
	vector<int> out(oSize);

	for ( size_t i = 0; i < oSize; ++i ){
		if ( i < a.size()){
			if( i < b.size()){
				out[i] = a[i] + b[i];
			}else{
				out[i] = a[i];
			}
		}else{
			if( i < b.size()){
				out[i] = b[i];
			}
		}
	}
	return out;
}


int main(int argc, char ** argv){
	// Init Faster Framework
	cout << "Init FastLib" << '\n';
	fastContext fc(argc,argv);

	fc.registerFunction((void*) &map1);
	fc.registerFunction((void*) &reduce1);

	fc.startWorkers();

	cout << "Generate Data" << '\n';
	vector<int> rawdata[NUMITEMS];

	// Create a random adjacency matrix
	for ( size_t i = 0; i < NUMITEMS; ++i ){
		
		// Random number of items
		size_t dataSize = (rand() % 10) + 1;

		rawdata[i].assign(dataSize, 2);
	}

	cout << "Import Data" << '\n';
	fdd <vector<int>> data(fc, rawdata, NUMITEMS);

	cout << "Process Data" << '\n';
	vector<int> result = data.map<vector<int>>(&map1)->reduce(&reduce1);

	cout << "DONE!" << '\n';

	for ( size_t i = 0; i < result.size(); ++i){
		cout << result[i] << ' ';
	}
	cout << '\n';


	return 0;
}
