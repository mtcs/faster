#include <iostream>
#include "libfaster.h"

#define NUMITEMS 100*1000

using namespace std;

int map1(string & input){
	return atoi(input.data());
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


	cout << "Import Data" << '\n';
	fdd<string> data(fc, "../res/testM.txt");

	cout << "Process Data" << '\n';
	int result = data.map<int>(&map1)->reduce(&reduce1);

	cout << "DONE!" << '\n';
	std::cout << result << "\n";

	return 0;
}
