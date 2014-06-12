#include <iostream>
#include <stdlib.h>
#include "libfaster.h"

#define NUMITEMS 100*1000

int map1(int & input){
	return input / 2;
}

int reduce1(int &a, int &b){
	return a + b;
}

void * funcTable [] = {
	(void *) &map1, 	// 0
	(void *) &reduce1	// 1
};

int main(int argc, char ** argv){
	fastContext fc("local", funcTable);

	int rawdata[NUMITEMS];

	for ( int i = 0; i < NUMITEMS; ++i )
		rawdata[i] = i+1;

	fdd <int> data(fc, rawdata, NUMITEMS);

	int result = data.map<int>(0)->reduce(1);

	std::cout << result << "\n";

	return EXIT_SUCCESS;
}
