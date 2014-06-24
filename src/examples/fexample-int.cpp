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
	// Init Faster Framework
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
	int result = data.map<int>(&map1)->reduce(&reduce1);

	cout << "DONE!" << '\n';

	std::cout << "Resut:" << result << "\n";

	return 0;
}
